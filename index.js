/**
 * Twilio + Grok Voice Bridge v2.4
 * Fix: configurable AUDIO_SOURCE_RATE (16k/24k/8k)
 * PCM16 @ 16kHz or 24kHz → μ-law 8kHz serial queue
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
  AUDIO_SOURCE_RATE = 16000   // 16000 | 24000 | 8000
} = process.env;

const sourceRate = parseInt(AUDIO_SOURCE_RATE, 10) || 16000;
const downsampleFactor = sourceRate >= 24000 ? 3 : (sourceRate >= 16000 ? 2 : 1);

console.log('[CONFIG] AUDIO_INPUT_FORMAT:', AUDIO_INPUT_FORMAT);
console.log('[CONFIG] AUDIO_SOURCE_RATE:', sourceRate, 'Hz');
console.log('[CONFIG] Downsample factor:', downsampleFactor, '(→ 8kHz)');

// ─── UTILITIES ───────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── G.711 μ-law encoding (standard G.711 section 2.2) ───────────────────────

const CLIP = 32635;
const BIAS = 33;

function pcm16ToUlaw(pcmBuf) {
  const out = Buffer.alloc(Math.floor(pcmBuf.length / 2));
  for (let i = 0; i < pcmBuf.length - 1; i += 2) {
    let sample = pcmBuf.readInt16LE(i);
    let sign = 0;
    if (sample < 0) { sign = 1; sample = -sample; }
    sample += BIAS;
    if (sample > CLIP) sample = CLIP;
    let exponent = 0;
    let mask = 16384;
    while (sample > mask && exponent <= 14) { mask >>= 1; exponent++; }
    let mantissa = (sample >> (exponent === 0 ? 4 : exponent + 3)) & 0x0F;
    let ulawbyte = (exponent << 4) | mantissa;
    ulawbyte = sign === 1 ? 0x80 | (0x7F - ulawbyte) : 0x80 | ulawbyte;
    out[Math.floor(i / 2)] = ulawbyte;
  }
  return out;
}

// ─── AUDIO CONVERTER + CHUNKER ────────────────────────────────────────────────

function convertAndChunkAudio(base64Audio) {
  const raw = Buffer.from(base64Audio, 'base64');
  console.log('[AUDIO] input bytes:', raw.length);

  // PCM16 input
  console.log('[AUDIO] Format mode:', AUDIO_INPUT_FORMAT);
  console.log('[AUDIO] Source rate:', sourceRate, 'Hz');
  console.log('[AUDIO] Downsample factor:', downsampleFactor);

  const numInputSamples = Math.floor(raw.length / 2);
  console.log('[AUDIO] PCM input samples:', numInputSamples);

  // Read PCM16 LE samples
  const samples = new Int16Array(numInputSamples);
  for (let i = 0; i < numInputSamples; i++) samples[i] = raw.readInt16LE(i * 2);

  // Downsample to 8kHz
  const numOutputSamples = Math.floor(samples.length / downsampleFactor);
  const outSamples = new Int16Array(numOutputSamples);
  for (let i = 0; i < numOutputSamples; i++) outSamples[i] = samples[i * downsampleFactor];

  console.log('[AUDIO] PCM8k samples:', numOutputSamples);

  // Build PCM16 8kHz buffer
  const pcm8k = Buffer.alloc(numOutputSamples * 2);
  for (let i = 0; i < numOutputSamples; i++) pcm8k.writeInt16LE(outSamples[i], i * 2);

  // Encode to μ-law
  const ulaw = pcm16ToUlaw(pcm8k);
  console.log('[AUDIO] μ-law bytes:', ulaw.length);

  // Chunk into 160-byte packets (20ms @ 8kHz = 160 samples = 160 bytes μ-law)
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
    grokWs: null,
    isGrokSessionReady: false,
    responseCount: 0
  };
}

// ─── SERIAL AUDIO SENDER ──────────────────────────────────────────────────────

async function startAudioSender(state) {
  if (state.isSendingAudio) {
    console.log('[AUDIO_QUEUE] sender already running, pending:', state.audioQueue.length);
    return;
  }
  state.isSendingAudio = true;
  console.log('[AUDIO_QUEUE] sender started, queue:', state.audioQueue.length);

  while (state.audioQueue.length > 0) {
    const chunk = state.audioQueue.shift();
    if (state.ws.readyState === WebSocket.OPEN && state.streamSid) {
      state.ws.send(JSON.stringify({
        event: 'media',
        streamSid: state.streamSid,
        media: { payload: chunk.toString('base64') }
      }));
      console.log('[TWILIO] sent sequential chunk 160 bytes');
    }
    await sleep(20);
  }

  state.isSendingAudio = false;
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

// ─── WEBSOCKET SERVER ─────────────────────────────────────────────────────────

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (ws, req) => {
  let state = null;

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      const event = data.event;

      if (event === 'start') {
        const streamSid = data.start?.streamSid;
        const callSid = data.start?.callSid;
        const customerPhone = data.start?.parameters?.From || 'unknown';
        const customerName = data.start?.customParameters?.customerName || 'Cliente';

        console.log('[WS] Stream start — streamSid:', streamSid, '| callSid:', callSid);
        console.log('[WS] Customer:', customerName, customerPhone);

        state = createCallState(ws, streamSid, callSid, customerPhone, customerName);
        connectToGrokVoice(state);

      } else if (event === 'media' && state) {
        const payload = data.media?.payload;
        if (payload && state.grokWs?.readyState === WebSocket.OPEN && state.isGrokSessionReady) {
          state.grokWs.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: payload }));
        }

      } else if (event === 'stop' && state) {
        console.log('[WS] Stream stopped — reason:', data.stop?.reason);
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
    if (state?.grokWs) { state.grokWs.close(); state.grokWs = null; }
    if (state) { state.isGrokSessionReady = false; }
  });

  ws.on('error', (err) => console.error('[WS] Error:', err.message));
});

// ─── GROK VOICE ───────────────────────────────────────────────────────────────

function connectToGrokVoice(state) {
  state.grokWs = new WebSocket(
    'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0',
    { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
  );

  state.grokWs.on('open', () => {
    console.log('[GROK] Connected');
    const prompt = `Você é a assistente virtual da Família do Gás, delivery de gás em Campo Grande, MS. Ligue de forma natural e amigável. Nunca diga que é IA. Voz Sal. Máximo 2 minutos.`;

    state.grokWs.send(JSON.stringify({
      type: 'session.update',
      session: {
        voice: 'Sal',
        instructions: prompt,
        turn_detection: { type: 'server_vad', threshold: 0.85, silence_duration_ms: 500 },
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
    console.log('[GROK] Session configured');

    state.grokWs.send(JSON.stringify({
      type: 'conversation.item.create',
      item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Olá, ligando da Família do Gás. Tem um minuto?' }] }
    }));
    state.grokWs.send(JSON.stringify({ type: 'response.create' }));
    console.log('[GROK] Greeting sent');
  });

  state.grokWs.on('message', (msg) => {
    try {
      const evt = JSON.parse(msg);
      const t = evt.type || '';

      if (t === 'session.updated') {
        state.isGrokSessionReady = true;
        console.log('[GROK] Session ready');

      } else if (t === 'response.output_audio.delta') {
        const audioBase64 = evt.delta;
        if (!audioBase64) { console.log('[GROK] empty audio delta'); return; }
        console.log('[GROK] audio delta received, len:', audioBase64.length);

        const chunks = convertAndChunkAudio(audioBase64);
        state.audioQueue.push(...chunks);
        console.log('[AUDIO_QUEUE] enqueued chunks:', chunks.length, '| pending:', state.audioQueue.length);
        startAudioSender(state);

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
        console.log('[GROK] VAD — client speaking, cancelling AI');
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

app.get('/health', (_, res) => res.json({ status: 'ok', v: '2.4', audio_fmt: AUDIO_INPUT_FORMAT, source_rate: sourceRate }));
app.get('/', (_, res) => res.json({ name: 'Twilio + Grok Voice Bridge', v: '2.4' }));

server.listen(PORT, () => {
  console.log(`\n🚀 Twilio + Grok Bridge v2.4 on :${PORT}`);
  console.log(`🔊 Audio: pcm16 @ ${sourceRate}Hz → μ-law 8kHz | serial queue | 20ms pacing`);
  console.log(`📐 Downsample: ${downsampleFactor}:1`);
});

module.exports = { app, server };