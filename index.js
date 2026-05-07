/**
 * Twilio + Grok Voice Bridge v2.7
 * BARGE-IN: real interruption when user speaks
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
  AUDIO_GAIN = 0.85,
  MAX_QUEUE_SIZE = 80
} = process.env;

const sourceRate = parseInt(AUDIO_SOURCE_RATE, 10) || 24000;
const downsampleFactor = sourceRate >= 24000 ? 3 : (sourceRate >= 16000 ? 2 : 1);
const gain = parseFloat(AUDIO_GAIN) || 0.85;
const PREBUFFER_CHUNKS = 4;
const MAX_QUEUE = parseInt(MAX_QUEUE_SIZE, 10) || 80;

console.log('[CONFIG] AUDIO_SOURCE_RATE:', sourceRate, 'Hz | gain:', gain, '| max queue:', MAX_QUEUE);

// ─── UTILITIES ───────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── G.711 μ-law ─────────────────────────────────────────────────────────────

const BIAS = 0x84;
const CLIP = 32635;

function linearToMulawSample(sample) {
  let g = sample * gain;
  if (g > 32767) g = 32767;
  if (g < -32768) g = -32768;
  let sign = (g < 0) ? 0x80 : 0;
  if (sign !== 0) g = -g;
  if (g > CLIP) g = CLIP;
  g += BIAS;
  let exponent = 7;
  for (let m = 0x4000; (g & m) === 0 && exponent > 0; exponent--, m >>= 1) {}
  const mantissa = (g >> (exponent + 3)) & 0x0F;
  return ~(sign | (exponent << 4) | mantissa) & 0xFF;
}

function pcm16ToUlaw(pcmBuf) {
  const out = Buffer.alloc(Math.floor(pcmBuf.length / 2));
  for (let i = 0; i < out.length; i++) out[i] = linearToMulawSample(pcmBuf.readInt16LE(i * 2));
  return out;
}

// ─── DOWNSAMPLE (3-sample average) ───────────────────────────────────────────

function downsampleAvg(samples, factor) {
  const out = new Int16Array(Math.floor(samples.length / factor));
  for (let i = 0; i < out.length; i++) {
    let sum = 0;
    for (let j = 0; j < factor; j++) sum += samples[i * factor + j];
    out[i] = Math.round(sum / factor);
  }
  return out;
}

// ─── AUDIO CONVERTER ───────────────────────────────────────────────────────────

function convertAndChunk(base64Audio) {
  const raw = Buffer.from(base64Audio, 'base64');
  const nIn = Math.floor(raw.length / 2);
  const samples = new Int16Array(nIn);
  for (let i = 0; i < nIn; i++) samples[i] = raw.readInt16LE(i * 2);
  const out8 = downsampleAvg(samples, downsampleFactor);
  const pcm8 = Buffer.alloc(out8.length * 2);
  for (let i = 0; i < out8.length; i++) pcm8.writeInt16LE(out8[i], i * 2);
  const ulaw = pcm16ToUlaw(pcm8);
  const chunks = [];
  for (let i = 0; i < ulaw.length; i += 160) chunks.push(ulaw.slice(i, i + 160));
  return chunks;
}

// ─── CALL STATE ───────────────────────────────────────────────────────────────

function createState(ws, streamSid, callSid, phone, name) {
  return {
    ws, streamSid, callSid, phone, name,
    audioQueue: [],
    isSendingAudio: false,
    prebufferCount: 0,
    grokWs: null,
    isGrokSessionReady: false,
    responseCount: 0,
    isCallActive: true,
    isAiSpeaking: false,
    isUserSpeaking: false,
    playbackGeneration: 0,
    grokResponseActive: false,
    timestamps: {}
  };
}

// ─── TWILIO SEND (with clear) ─────────────────────────────────────────────────

function twilioClear(state) {
  if (state.ws?.readyState === WebSocket.OPEN && state.streamSid) {
    state.ws.send(JSON.stringify({ event: 'clear', streamSid: state.streamSid }));
    console.log('[BARGE_IN] Twilio clear sent');
  }
}

// ─── AUDIO SENDER WITH BARGE-IN CHECK ────────────────────────────────────────

async function sendAudio(state) {
  if (state.isSendingAudio) return;
  state.isSendingAudio = true;

  // Prebuffer
  state.prebufferCount = 0;
  while (state.prebufferCount < PREBUFFER_CHUNKS && state.audioQueue.length > 0) {
    await sleep(20);
    state.prebufferCount++;
    console.log('[AUDIO] prebuffer:', state.prebufferCount, '/', PREBUFFER_CHUNKS);
  }
  console.log('[AUDIO_QUEUE] prebuffer ready, queue:', state.audioQueue.length);

  const gen = state.playbackGeneration;

  while (state.audioQueue.length > 0) {
    // BARGE-IN: stop if user is speaking or generation changed
    if (!state.isCallActive || state.isUserSpeaking || state.playbackGeneration !== gen) {
      console.log('[AUDIO_QUEUE] stopped due to barge-in');
      state.audioQueue = [];
      break;
    }
    const chunk = state.audioQueue.shift();
    if (state.ws.readyState === WebSocket.OPEN && state.streamSid) {
      state.ws.send(JSON.stringify({
        event: 'media',
        streamSid: state.streamSid,
        media: { payload: chunk.toString('base64') }
      }));
    }
    await sleep(20);
  }

  state.isSendingAudio = false;
  console.log('[AUDIO_QUEUE] drained');
}

// ─── BARGE-IN HANDLER ─────────────────────────────────────────────────────────

function handleBargeIn(state) {
  if (state.isUserSpeaking) return; // already in barge-in

  console.log('[BARGE_IN] user started speaking');
  state.isUserSpeaking = true;
  state.isAiSpeaking = false;

  // Clear Twilio audio buffer
  twilioClear(state);
  console.log('[BARGE_IN] audio queue cleared');
  console.log('[BARGE_IN] Twilio clear sent');

  // Cancel Grok response if active
  if (state.grokResponseActive && state.grokWs?.readyState === WebSocket.OPEN) {
    state.grokWs.send(JSON.stringify({ type: 'response.cancel' }));
    state.grokResponseActive = false;
    console.log('[GROK] response cancelled due to barge-in');
  }

  // Increment generation to abort sender
  state.playbackGeneration++;
  state.audioQueue = [];
  console.log('[BARGE_IN] generation incremented to', state.playbackGeneration);
}

// ─── TWILIO WEBHOOKS ─────────────────────────────────────────────────────────

app.post('/voice', (req, res) => {
  console.log('[TWILIO] /voice —', req.body?.CallSid);
  res.type('text/xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<Response><Connect><Stream url="wss://twilio.salestecnologia.com.br/grok-media-stream"/></Connect></Response>`);
});

app.post('/voice-say', (_, res) => {
  res.type('text/xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<Response><Say language="pt-BR" voice="alice">Teste de audio.</Say></Response>`);
});

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (ws, req) => {
  let state = null;

  ws.on('message', msg => {
    try {
      const d = JSON.parse(msg);
      if (d.event === 'start') {
        state = createState(ws, d.start?.streamSid, d.start?.callSid,
          d.start?.parameters?.From || 'unk', d.start?.customParameters?.customerName || 'Client');
        state.timestamps.callStart = Date.now();
        console.log('[WS] start —', state.streamSid);
        connectGrok(state);

      } else if (d.event === 'media' && state) {
        if (d.media?.payload && state.grokWs?.readyState === WebSocket.OPEN && state.isGrokSessionReady) {
          state.grokWs.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: d.media.payload }));
        }

      } else if (d.event === 'stop' && state) {
        console.log('[WS] stop');
        state.isCallActive = false;
        state.audioQueue = [];
        state.isUserSpeaking = false;
        state.isAiSpeaking = false;

      } else if (d.event === 'mark' && state) {
        console.log('[WS] mark:', d.mark?.name);
      }
    } catch (e) { console.error('[WS]', e.message); }
  });

  ws.on('close', () => {
    console.log('[WS] disconnect');
    if (state) { state.isCallActive = false; state.audioQueue = []; }
  });
});

// ─── GROK ─────────────────────────────────────────────────────────────────────

function connectGrok(state) {
  state.grokWs = new WebSocket(
    'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0',
    { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
  );

  state.grokWs.on('open', () => {
    console.log('[GROK] connected');
    const prompt = `Você é a assistente virtual da Família do Gás, delivery de gás em Campo Grande, MS. Ligando para clientes inativos.

## REGRAS
- Frases curtas, menos de 12 palavras
- Uma pergunta por vez
- NÃO diga "do que se trata?"
- NÃO pergunte genericamente

## ROTEIRO
1. "Olá, aqui é da Família do Gás. Você está precisando de gás?"
2. Se "sim" → "É para o mesmo endereço?"
3. Se "não" → "Quer que te lembre outro dia?"
4. Encerrar → "Ligue no WhatsApp: 67 9361-8055"

## FERRAMENTA
register_result(intent, notes)
intent: venda | interesse | agendar | sem_interesse | sem_resposta`;

    state.grokWs.send(JSON.stringify({
      type: 'session.update',
      session: {
        voice: 'Sal',
        instructions: prompt,
        turn_detection: { type: 'server_vad', threshold: 0.5, silence_duration_ms: 500, prefix_padding_ms: 200 },
        tools: [{ type: 'function', name: 'register_result', description: 'registra resultado',
          parameters: { type: 'object', properties: { intent: { type: 'string', enum: ['venda','interesse','agendar','sem_interesse','sem_resposta'] }, notes: { type: 'string' } }, required: ['intent'] } }],
        input_audio_transcription: { model: 'grok-2-audio' },
        audio: { input: { format: { type: 'audio/pcm', rate: 24000 } }, output: { format: { type: 'audio/pcm', rate: 24000 } } }
      }
    }));

    state.grokWs.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Inicie.' }] } }));
    state.grokWs.send(JSON.stringify({ type: 'response.create' }));
    console.log('[GROK] session + greeting sent');
  });

  state.grokWs.on('message', msg => {
    try {
      const e = JSON.parse(msg);
      const t = e.type || '';

      if (t === 'session.updated') {
        state.isGrokSessionReady = true;

      } else if (t === 'response.output_audio.delta') {
        if (!e.delta || !state.isCallActive) return;
        state.isAiSpeaking = true;
        state.grokResponseActive = true;
        const chunks = convertAndChunk(e.delta);

        // Limit queue to MAX_QUEUE
        if (state.audioQueue.length > MAX_QUEUE) {
          console.log('[AUDIO_QUEUE] overflow, trimming from', state.audioQueue.length, 'to', MAX_QUEUE);
          state.audioQueue = state.audioQueue.slice(-MAX_QUEUE);
        }

        state.audioQueue.push(...chunks);
        console.log('[AUDIO_QUEUE] queued:', chunks.length, '| pending:', state.audioQueue.length);
        sendAudio(state);

      } else if (t === 'response.output_audio_transcript.delta') {
        process.stdout.write(e.delta);

      } else if (t === 'response.done') {
        state.responseCount++;
        state.isAiSpeaking = false;
        state.grokResponseActive = false;
        console.log(`[GROK] response done (${state.responseCount})`);

      } else if (t === 'input_audio_buffer.speech_started') {
        // BARGE-IN triggered
        handleBargeIn(state);

      } else if (t === 'input_audio_buffer.speech_stopped') {
        console.log('[BARGE_IN] user stopped speaking, waiting');
        state.isUserSpeaking = false;

      } else if (t === 'response.function_call_arguments.done') {
        const args = JSON.parse(e.arguments || '{}');
        console.log('[TOOL]', e.name, args);
        if (e.name === 'register_result') sendResult(state.callSid, state.phone, state.customerName, args.intent, args.notes || '');
        state.grokWs.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'function_call_output', call_id: e.call_id, output: JSON.stringify({ success: true }) } }));
        state.grokWs.send(JSON.stringify({ type: 'response.create' }));

      } else if (t === 'conversation.item.input_audio_transcription.completed') {
        console.log('[GROK] client:', e.transcript);

      } else if (t === 'error') {
        console.error('[GROK]', e.error?.message);
      }
    } catch (err) { console.error('[GROK] parse', err.message); }
  });

  state.grokWs.on('close', () => { state.isGrokSessionReady = false; state.grokResponseActive = false; console.log('[GROK] close'); });
  state.grokWs.on('error', err => console.error('[GROK]', err.message));
}

server.on('upgrade', (req, socket, head) => {
  if (req.url === '/grok-media-stream') wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  else socket.destroy();
});

// ─── OUTBOUND + N8N + HEALTH ───────────────────────────────────────────────────

app.post('/call', async (req, res) => {
  const { to } = req.body;
  if (!to) return res.status(400).json({ error: 'Missing "to"' });
  try {
    const c = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    const call = await c.calls.create({ to, from: TWILIO_PHONE_NUMBER, url: `https://${req.headers.host}/voice`, statusCallback: `https://${req.headers.host}/voice/call-status`, statusCallbackEvent: ['completed', 'no-answer', 'busy'] });
    res.json({ success: true, callSid: call.sid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/call-test', async (req, res) => {
  const { to } = req.body;
  if (!to) return res.status(400).json({ error: 'Missing "to"' });
  try {
    const c = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    const call = await c.calls.create({ to, from: TWILIO_PHONE_NUMBER, url: `https://${req.headers.host}/voice-say`, statusCallback: `https://${req.headers.host}/voice/call-status`, statusCallbackEvent: ['completed', 'no-answer', 'busy'] });
    res.json({ success: true, callSid: call.sid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/calls', async (req, res) => {
  const c = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
  const calls = await c.calls.list({ limit: 20 });
  res.json({ calls: calls.map(x => ({ sid: x.sid, from: x.from, to: x.to, status: x.status, duration: x.duration })) });
});

async function sendResult(callSid, phone, name, intent, notes) {
  if (!N8N_WEBHOOK_URL) return;
  try {
    const r = await fetch(N8N_WEBHOOK_URL, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source: 'twilio_grok', call_sid: callSid, customer_name: name, phone, intent, notes }) });
    console.log('[N8N] ok:', r.ok);
  } catch (e) { console.error('[N8N]', e.message); }
}

app.get('/health', (_, r) => r.json({ status: 'ok', v: '2.7', gain, maxQueue: MAX_QUEUE }));
app.get('/', (_, r) => r.json({ name: 'Twilio + Grok Bridge', v: '2.7' }));

server.listen(PORT, () => {
  console.log(`\n🚀 v2.7 on :${PORT} | gain:${gain} | max queue:${MAX_QUEUE} | BARGE-IN enabled`);
});

module.exports = { app, server };