/**
 * Twilio + Grok Voice Bridge v2.6
 * Fixes: prebuffer, audio gain, better downsampling, faster VAD, commercial context
 */

require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const http = require('http');

const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const {
  TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER,
  XAI_API_KEY, N8N_WEBHOOK_URL, PORT = 3000,
  AUDIO_INPUT_FORMAT = 'pcm16',
  AUDIO_SOURCE_RATE = 24000,
  AUDIO_GAIN = 0.85
} = process.env;

const sourceRate = parseInt(AUDIO_SOURCE_RATE, 10) || 24000;
const downsampleFactor = sourceRate >= 24000 ? 3 : (sourceRate >= 16000 ? 2 : 1);
const gain = parseFloat(AUDIO_GAIN) || 0.85;
const PREBUFFER_CHUNKS = 4; // accumulate 4 chunks before starting sender

console.log('[CONFIG] AUDIO_INPUT_FORMAT:', AUDIO_INPUT_FORMAT);
console.log('[CONFIG] AUDIO_SOURCE_RATE:', sourceRate, 'Hz');
console.log('[CONFIG] Downsample factor:', downsampleFactor, '(→ 8kHz)');
console.log('[CONFIG] AUDIO_GAIN:', gain);
console.log('[AUDIO] using standard G.711 μ-law encoder');
console.log('[AUDIO] Downsampling: 3-sample averaging');

// ─── UTILITIES ───────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── STANDARD G.711 μ-law encoder ───────────────────────────────────────────

const BIAS = 0x84;
const CLIP = 32635;

function linearToMulawSample(sample) {
  // Apply gain
  let g = sample * gain;
  // Clamp
  if (g > 32767) g = 32767;
  if (g < -32768) g = -32768;

  let sign = (g < 0) ? 0x80 : 0;
  if (sign !== 0) g = -g;
  if (g > CLIP) g = CLIP;
  g = g + BIAS;

  let exponent = 7;
  for (let expMask = 0x4000; (g & expMask) === 0 && exponent > 0; exponent--, expMask >>= 1) {}

  const mantissa = (g >> (exponent + 3)) & 0x0F;
  const ulawByte = ~(sign | (exponent << 4) | mantissa);
  return ulawByte & 0xFF;
}

function pcm16ToUlaw(pcmBuf) {
  const out = Buffer.alloc(Math.floor(pcmBuf.length / 2));
  for (let i = 0; i < out.length; i++) {
    const sample = pcmBuf.readInt16LE(i * 2);
    out[i] = linearToMulawSample(sample);
  }
  return out;
}

// ─── BETTER DOWNSAMPLING (3-sample averaging) ────────────────────────────────

function downsampleAverage(samples, factor) {
  const numOut = Math.floor(samples.length / factor);
  const out = new Int16Array(numOut);
  for (let i = 0; i < numOut; i++) {
    const start = i * factor;
    let sum = 0;
    for (let j = 0; j < factor; j++) {
      sum += samples[start + j];
    }
    out[i] = Math.round(sum / factor);
  }
  return out;
}

// ─── AUDIO CONVERTER + CHUNKER ────────────────────────────────────────────────

function convertAndChunkAudio(base64Audio) {
  const raw = Buffer.from(base64Audio, 'base64');
  const numInputSamples = Math.floor(raw.length / 2);

  console.log('[AUDIO] input bytes:', raw.length);
  console.log('[AUDIO] PCM input samples:', numInputSamples);

  // Read PCM16 LE samples
  const samples = new Int16Array(numInputSamples);
  for (let i = 0; i < numInputSamples; i++) samples[i] = raw.readInt16LE(i * 2);

  // Downsample using 3-sample averaging (reduces noise)
  const outSamples = downsampleAverage(samples, downsampleFactor);
  console.log('[AUDIO] PCM8k samples:', outSamples.length);

  // Build PCM16 8kHz buffer
  const pcm8k = Buffer.alloc(outSamples.length * 2);
  for (let i = 0; i < outSamples.length; i++) pcm8k.writeInt16LE(outSamples[i], i * 2);

  // Encode to μ-law with standard G.711
  const ulaw = pcm16ToUlaw(pcm8k);
  console.log('[AUDIO] μ-law bytes:', ulaw.length);

  // Chunk into 160-byte packets (20ms @ 8kHz)
  const CHUNK = 160;
  const chunks = [];
  for (let i = 0; i < ulaw.length; i += CHUNK) chunks.push(ulaw.slice(i, i + CHUNK));
  console.log('[AUDIO] Chunks:', chunks.length, 'x 160 bytes');
  return chunks;
}

// ─── CALL STATE ───────────────────────────────────────────────────────────────

function createCallState(ws, streamSid, callSid, customerPhone, customerName) {
  return {
    ws, streamSid, callSid, customerPhone, customerName,
    audioQueue: [],
    isSendingAudio: false,
    prebufferCount: 0,
    grokWs: null,
    isGrokSessionReady: false,
    responseCount: 0,
    isCallActive: true,
    timestamps: {}
  };
}

// ─── SERIAL AUDIO SENDER WITH PREBUFFER ──────────────────────────────────────

async function startAudioSender(state) {
  if (state.isSendingAudio) {
    console.log('[AUDIO_QUEUE] sender already running, pending:', state.audioQueue.length);
    return;
  }

  // PREBUFFER: accumulate chunks before sending
  state.prebufferCount = 0;
  const waitForPrebuffer = () => {
    return new Promise(resolve => {
      const check = () => {
        state.prebufferCount++;
        console.log('[AUDIO_QUEUE] prebuffer:', state.prebufferCount, '/', PREBUFFER_CHUNKS, '| queue:', state.audioQueue.length);
        if (state.prebufferCount >= PREBUFFER_CHUNKS || state.audioQueue.length === 0) {
          resolve();
        } else {
          setTimeout(check, 20);
        }
      };
      check();
    });
  };

  console.log('[AUDIO_QUEUE] starting sender with prebuffer (target:', PREBUFFER_CHUNKS, ')');
  state.isSendingAudio = true;

  // Wait for prebuffer
  await waitForPrebuffer();

  if (state.audioQueue.length === 0) {
    state.isSendingAudio = false;
    console.log('[AUDIO_QUEUE] prebuffer done but queue empty, aborting');
    return;
  }

  console.log('[AUDIO_QUEUE] prebuffer ready:', state.audioQueue.length, 'chunks');

  while (state.audioQueue.length > 0) {
    // Check if call is still active before each send
    if (!state.isCallActive || state.ws.readyState !== WebSocket.OPEN) {
      console.log('[AUDIO_QUEUE] stopped because call ended');
      state.audioQueue = [];
      break;
    }

    const chunk = state.audioQueue.shift();
    state.ws.send(JSON.stringify({
      event: 'media',
      streamSid: state.streamSid,
      media: { payload: chunk.toString('base64') }
    }));
    console.log('[TWILIO] sent sequential chunk 160 bytes');
    await sleep(20);
  }

  state.isSendingAudio = false;
  state.prebufferCount = 0;
  console.log('[AUDIO_QUEUE] drained');
}

// ─── TWILIO WEBHOOKS ──────────────────────────────────────────────────────────

app.post('/voice', (req, res) => {
  console.log('[TWILIO] POST /voice — CallSid:', req.body?.CallSid, 'From:', req.body?.From);
  const twiML = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://twilio.salestecnologia.com.br/grok-media-stream"/>
  </Connect>
</Response>`;
  res.type('text/xml').send(twiML);
});

app.post('/voice/call-status', (req, res) => {
  console.log('[TWILIO] Status:', req.body?.CallStatus, '| Sid:', req.body?.CallSid);
  res.sendStatus(200);
});

// ─── TEST ENDPOINT ─────────────────────────────────────────────────────────────

app.post('/voice-say', (req, res) => {
  console.log('[TWILIO] POST /voice-say — test audio');
  const twiML = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say language="pt-BR" voice="alice">Teste de audio Twilio. Se voce ouvir essa mensagem com clareza, o problema esta na conversao do bridge.</Say>
</Response>`;
  res.type('text/xml').send(twiML);
});

// ─── WEBSOCKET SERVER ─────────────────────────────────────────────────────────

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (ws, req) => {
  let state = null;
  const callStartTime = Date.now();

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      const event = data.event;

      if (event === 'start') {
        const streamSid = data.start?.streamSid;
        const callSid = data.start?.callSid;
        const customerPhone = data.start?.parameters?.From || 'unknown';
        const customerName = data.start?.customParameters?.customerName || 'Cliente';

        state = createCallState(ws, streamSid, callSid, customerPhone, customerName);
        state.timestamps.callStartMs = Date.now();
        console.log('[WS] Stream start — streamSid:', streamSid, '| callSid:', callSid);
        console.log('[WS] Customer:', customerName, customerPhone);
        console.log('[LATENCY] twilio_start_to_grok_ready_ms:', '(measured on grok connect)');

        connectToGrokVoice(state);

      } else if (event === 'media' && state) {
        const payload = data.media?.payload;
        const recvTime = Date.now();
        if (payload && state.grokWs?.readyState === WebSocket.OPEN && state.isGrokSessionReady) {
          state.grokWs.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: payload }));
          if (state.timestamps.lastUserAudioMs) {
            console.log('[LATENCY] user_audio_to_response_ms:', recvTime - state.timestamps.lastUserAudioMs);
          }
          state.timestamps.lastUserAudioMs = recvTime;
        }

      } else if (event === 'stop' && state) {
        console.log('[WS] stop received, clearing audio queue');
        console.log('[WS] Stream stopped — reason:', data.stop?.reason);
        state.isCallActive = false;
        state.audioQueue = [];
        if (state.grokWs) { state.grokWs.close(); state.grokWs = null; }
        state.isGrokSessionReady = false;

      } else if (event === 'dtmf' && state) {
        console.log('[WS] DTMF:', data.dtmf?.digit);
      }
    } catch (e) {
      console.error('[WS] Error:', e.message);
    }
  });

  ws.on('close', () => {
    console.log('[WS] Twilio disconnected');
    if (state) {
      state.isCallActive = false;
      state.audioQueue = [];
      if (state.grokWs) { state.grokWs.close(); state.grokWs = null; }
      state.isGrokSessionReady = false;
    }
  });

  ws.on('error', (err) => console.error('[WS] Error:', err.message));
});

// ─── GROK VOICE ───────────────────────────────────────────────────────────────

function connectToGrokVoice(state) {
  state.timestamps.grokConnectMs = Date.now();
  console.log('[LATENCY] twilio_start_to_grok_ready_ms:', state.timestamps.grokConnectMs - state.timestamps.callStartMs);

  state.grokWs = new WebSocket(
    'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0',
    { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
  );

  state.grokWs.on('open', () => {
    console.log('[GROK] Connected');

    // Updated persona: direct commercial approach, no "do que se trata?"
    const agentInstructions = `Você é a assistente virtual da Família do Gás, empresa de delivery de gás em Campo Grande, MS. Você está ligando para clientes que estão inativos há mais de 15 dias.

## REGRAS DE OUVIDO
- Fale de forma natural, curta e direta
- Uma pergunta por vez
- Menos de 12 palavras por frase
- Nunca diga "do que se trata?" ou "em que posso ajudar?"
- Nunca pergunte genericamente

## ROTEIRO

### ABERTURA (primeiros 5 segundos):
"Olá, aqui é da Família do Gás. Tudo bem? Eu estou ligando porque percibemos que faz um tempo do seu último pedido. Você está precisando de gás hoje?"

### SE CLIENTE DIZER "sim", "está tudo bem", "pode ser":
"Fechou! Só confirmar: é para o endereço habitual, né? Em quanto tempo você precisa?"

### SE CLIENTE DIZER "não", "não preciso":
"Tudo bem, sem problema. Só para saber, você costuma comprar de qual distribuidores? A gente tem uma promoção especial para clientes antigos."

### SE CLIENTE NÃO ATENDER OU DESVIAR:
"Se não estiver num bom momento, pode ligar depois neste número: 67 9361-8055. Obrigado!"

### ENCERRAMENTO:
"Se precisar, é só chamar no WhatsApp: 67 9361-8055. Muito obrigado, bom dia!"

## FERRAMENTA
register_result(intent, notes):
- intent: "venda" | "interesse" | "agendar" | "sem_interesse" | "sem_resposta"
- notes: observaciones`;

    state.grokWs.send(JSON.stringify({
      type: 'session.update',
      session: {
        voice: 'Sal',
        instructions: agentInstructions,
        turn_detection: {
          type: 'server_vad',
          threshold: 0.5,
          silence_duration_ms: 500,
          prefix_padding_ms: 200
        },
        tools: [{
          type: 'function',
          name: 'register_result',
          description: 'Registra resultado da chamada',
          parameters: {
            type: 'object',
            properties: {
              intent: { type: 'string', enum: ['venda', 'interesse', 'agendar', 'sem_interesse', 'sem_resposta'] },
              notes: { type: 'string' }
            },
            required: ['intent']
          }
        }],
        input_audio_transcription: { model: 'grok-2-audio' },
        audio: {
          input: { format: { type: 'audio/pcm', rate: 24000 } },
          output: { format: { type: 'audio/pcm', rate: 24000 } }
        }
      }
    }));
    console.log('[GROK] Session configured (faster VAD: threshold=0.5, silence=500ms)');

    state.grokWs.send(JSON.stringify({
      type: 'conversation.item.create',
      item: {
        type: 'message', role: 'user',
        content: [{ type: 'input_text', text: 'Inicie a ligação.' }]
      }
    }));
    state.grokWs.send(JSON.stringify({ type: 'response.create' }));
    console.log('[GROK] Greeting sent');

    state.timestamps.firstResponseSentMs = Date.now();
    console.log('[LATENCY] grok_ready_to_first_audio_ms:', state.timestamps.firstResponseSentMs - state.timestamps.grokConnectMs);
  });

  state.grokWs.on('message', (msg) => {
    try {
      const evt = JSON.parse(msg);
      const t = evt.type || '';

      if (t === 'session.updated') {
        state.isGrokSessionReady = true;

      } else if (t === 'response.output_audio.delta') {
        const audioBase64 = evt.delta;
        if (!audioBase64) return;
        if (!state.isCallActive) return;

        const firstAudioTime = Date.now();
        if (state.timestamps.firstAudioMs === undefined) {
          state.timestamps.firstAudioMs = firstAudioTime;
          console.log('[LATENCY] grok_ready_to_first_audio_ms:', firstAudioTime - state.timestamps.firstResponseSentMs);
          console.log('[LATENCY] first_audio_to_first_twilio_chunk_ms: (sending...)');
        }

        console.log('[GROK] audio delta received, len:', audioBase64.length);
        const chunks = convertAndChunkAudio(audioBase64);
        state.audioQueue.push(...chunks);
        console.log('[AUDIO_QUEUE] enqueued chunks:', chunks.length, '| pending:', state.audioQueue.length);
        startAudioSender(state);

        if (state.timestamps.firstTwilioChunkMs === undefined && chunks.length > 0) {
          console.log('[LATENCY] first_audio_to_first_twilio_chunk_ms:', Date.now() - state.timestamps.firstAudioMs);
        }

      } else if (t === 'response.output_audio_transcript.delta') {
        process.stdout.write(evt.delta);

      } else if (t === 'response.done') {
        state.responseCount++;
        console.log(`\n[CALL] Response complete (${state.responseCount}) — keeping call open, waiting for client`);

      } else if (t === 'response.function_call_arguments.done') {
        const name = evt.name;
        const args = JSON.parse(evt.arguments || '{}');
        console.log(`[TOOL] ${name}:`, args);
        if (name === 'register_result') {
          sendResultToN8N(state.callSid, state.customerPhone, state.customerName, args.intent, args.notes || '');
        }
        state.grokWs.send(JSON.stringify({
          type: 'conversation.item.create',
          item: { type: 'function_call_output', call_id: evt.call_id, output: JSON.stringify({ success: true }) }
        }));
        state.grokWs.send(JSON.stringify({ type: 'response.create' }));

      } else if (t === 'input_audio_buffer.speech_started') {
        console.log('[GROK] VAD — client speaking');
        if (state.grokWs?.readyState === WebSocket.OPEN) state.grokWs.send(JSON.stringify({ type: 'response.cancel' }));

      } else if (t === 'conversation.item.input_audio_transcription.completed') {
        console.log('[GROK] Client said:', evt.transcript);

      } else if (t === 'error') {
        console.error('[GROK] Error:', evt.error?.message);
      }
    } catch (e) {
      console.error('[GROK] Parse error:', e.message);
    }
  });

  state.grokWs.on('close', () => {
    console.log('[GROK] Closed');
    state.isGrokSessionReady = false;
  });

  state.grokWs.on('error', (err) => console.error('[GROK] Error:', err.message));
}

server.on('upgrade', (req, socket, head) => {
  if (req.url === '/grok-media-stream') {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  } else socket.destroy();
});

// ─── OUTBOUND CALL ────────────────────────────────────────────────────────────

app.post('/call', async (req, res) => {
  const { to, customerName, customerId } = req.body;
  if (!to) return res.status(400).json({ error: 'Missing "to"' });
  try {
    const client = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    const call = await client.calls.create({
      to, from: TWILIO_PHONE_NUMBER,
      url: `https://${req.headers.host}/voice`,
      statusCallback: `https://${req.headers.host}/voice/call-status`,
      statusCallbackEvent: ['completed', 'no-answer', 'busy']
    });
    console.log(`[OUTBOUND] ${call.sid} → ${to}`);
    res.json({ success: true, callSid: call.sid });
  } catch (err) {
    console.error('[OUTBOUND]', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/call-test', async (req, res) => {
  const { to, customerName } = req.body;
  if (!to) return res.status(400).json({ error: 'Missing "to"' });
  try {
    const client = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    const call = await client.calls.create({
      to, from: TWILIO_PHONE_NUMBER,
      url: `https://${req.headers.host}/voice-say`,
      statusCallback: `https://${req.headers.host}/voice/call-status`,
      statusCallbackEvent: ['completed', 'no-answer', 'busy']
    });
    console.log(`[OUTBOUND TEST] ${call.sid} → ${to} (Twilio TTS)`);
    res.json({ success: true, callSid: call.sid, note: 'Testing Twilio TTS' });
  } catch (err) {
    console.error('[OUTBOUND]', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/calls', async (req, res) => {
  try {
    const client = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    const calls = await client.calls.list({ limit: 20 });
    res.json({ calls: calls.map(c => ({ sid: c.sid, from: c.from, to: c.to, status: c.status, duration: c.duration })) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── N8N WEBHOOK ──────────────────────────────────────────────────────────────

async function sendResultToN8N(callSid, phone, name, intent, notes) {
  if (!N8N_WEBHOOK_URL) return;
  try {
    const r = await fetch(N8N_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source: 'twilio_grok', call_sid: callSid, customer_name: name, phone, intent, notes })
    });
    console.log('[N8N] Result sent:', r.ok);
  } catch (e) { console.error('[N8N]', e.message); }
}

// ─── HEALTH ───────────────────────────────────────────────────────────────────

app.get('/health', (_, res) => res.json({ status: 'ok', v: '2.6', gain, source_rate: sourceRate }));
app.get('/', (_, res) => res.json({ name: 'Twilio + Grok Voice Bridge', v: '2.6' }));

server.listen(PORT, () => {
  console.log(`\n🚀 Twilio + Grok Bridge v2.6 on :${PORT}`);
  console.log(`🔊 Audio: pcm16 @ ${sourceRate}Hz → std G.711 μ-law 8kHz | gain:${gain} | 3-sample avg downsample`);
  console.log(`📦 Prebuffer: ${PREBUFFER_CHUNKS} chunks | 20ms pacing`);
  console.log(`⚡ VAD: threshold=0.5, silence=500ms, padding=200ms`);
});

module.exports = { app, server };