/**
 * Twilio + Grok Voice Bridge v2.2
 * Serial audio queue per call — eliminates concurrent chunk sends
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
  XAI_API_KEY, N8N_WEBHOOK_URL, PORT = 3000
} = process.env;

// ─── UTILITIES ───────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── G.711 μ-law encoding ────────────────────────────────────────────────────

function pcm16ToUlaw(pcmBuf) {
  const out = Buffer.alloc(Math.floor(pcmBuf.length / 2));
  for (let i = 0; i < pcmBuf.length - 1; i += 2) {
    const sample = pcmBuf.readInt16LE(i);
    const abs = Math.abs(sample);
    let exp = 0, mask = 0x2000;
    while (abs > mask && exp < 15) { mask >>= 1; exp++; }
    let mantissa = exp === 0 ? abs >> 4 : abs >> (exp + 3);
    if (mantissa > 15) mantissa = 15;
    let ulawbyte = (exp << 4) | mantissa;
    ulawbyte = sample < 0 ? 0x80 | (0x7F - ulawbyte) : 0x80 | ulawbyte;
    out[Math.floor(i / 2)] = ulawbyte;
  }
  return out;
}

// ─── AUDIO CONVERTER + CHUNKER ───────────────────────────────────────────────

function convertAndChunkAudio(base64Audio) {
  const raw = Buffer.from(base64Audio, 'base64');
  console.log('[AUDIO] input bytes:', raw.length);

  let pcm8k;
  if (raw.length % 3 === 0) {
    console.log('[AUDIO] Format: PCM24');
    const n24 = Math.floor(raw.length / 3);
    const s24 = new Int16Array(n24);
    for (let i = 0; i < n24; i++) {
      const b0 = raw[i*3], b1 = raw[i*3+1], b2 = raw[i*3+2];
      let s = b0 | (b1 << 8) | ((b2 << 16) & 0x7F0000);
      if (s & 0x800000) s |= ~0xFFFFFF;
      s24[i] = s >> 8;
    }
    const n8 = Math.floor(s24.length / 3);
    const s8 = new Int16Array(n8);
    for (let i = 0; i < n8; i++) s8[i] = s24[i * 3];
    pcm8k = Buffer.alloc(n8 * 2);
    for (let i = 0; i < n8; i++) pcm8k.writeInt16LE(s8[i], i * 2);

  } else if (raw.length % 2 === 0) {
    console.log('[AUDIO] Format: PCM16 LE');
    const n24 = Math.floor(raw.length / 2);
    const s24 = new Int16Array(n24);
    for (let i = 0; i < n24; i++) s24[i] = raw.readInt16LE(i * 2);
    const n8 = Math.floor(s24.length / 3);
    const s8 = new Int16Array(n8);
    for (let i = 0; i < n8; i++) s8[i] = s24[i * 3];
    pcm8k = Buffer.alloc(n8 * 2);
    for (let i = 0; i < n8; i++) pcm8k.writeInt16LE(s8[i], i * 2);

  } else {
    console.log('[AUDIO] Format: unknown, treating as raw PCM16');
    pcm8k = raw;
  }

  console.log('[AUDIO] PCM16 8kHz samples:', Math.floor(pcm8k.length / 2));
  const ulaw = pcm16ToUlaw(pcm8k);

  const CHUNK = 160;
  const chunks = [];
  for (let i = 0; i < ulaw.length; i += CHUNK) chunks.push(ulaw.slice(i, i + CHUNK));
  console.log('[AUDIO] μ-law chunks:', chunks.length, 'x 160 bytes');
  return chunks;
}

// ─── CALL STATE FACTORY ───────────────────────────────────────────────────────

function createCallState(ws, streamSid, callSid, customerPhone, customerName) {
  return {
    ws,
    streamSid,
    callSid,
    customerPhone,
    customerName,
    audioQueue: [],
    isSendingAudio: false,
    grokWs: null,
    isGrokSessionReady: false,
    responseCount: 0
  };
}

// ─── SERIAL AUDIO SENDER (one per call) ───────────────────────────────────────

async function startAudioSender(state) {
  if (state.isSendingAudio) {
    console.log('[AUDIO_QUEUE] sender already running, queue has', state.audioQueue.length, 'pending');
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

// ─── GROK VOICE CONNECTION ────────────────────────────────────────────────────

function connectToGrokVoice(state) {
  state.grokWs = new WebSocket(
    'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0',
    { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
  );

  state.grokWs.on('open', () => {
    console.log('[GROK] Connected');
    const prompt = `Você é a assistente virtual da Família do Gás, delivery de gás em Campo Grande, MS. Ligue para o cliente de forma natural e amigável. Nunca diga que é IA. Voz Sal. Máximo 2 minutos.`;

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

    // Trigger greeting
    state.grokWs.send(JSON.stringify({
      type: 'conversation.item.create',
      item: {
        type: 'message', role: 'user',
        content: [{ type: 'input_text', text: 'Olá, ligando da Família do Gás. Tem um minuto?' }]
      }
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
        // Queue audio chunks — serial sender handles dispatch
        const audioBase64 = evt.delta;
        if (!audioBase64) { console.log('[GROK] empty audio delta'); return; }
        console.log('[GROK] audio delta received, len:', audioBase64.length);

        const chunks = convertAndChunkAudio(audioBase64);
        state.audioQueue.push(...chunks);
        console.log('[AUDIO_QUEUE] enqueued chunks:', chunks.length);
        console.log('[AUDIO_QUEUE] pending chunks:', state.audioQueue.length);

        startAudioSender(state);

      } else if (t === 'response.output_audio_transcript.delta') {
        process.stdout.write(evt.delta);

      } else if (t === 'response.done') {
        state.responseCount++;
        console.log(`\n[CALL] Response complete (${state.responseCount}) — keeping call open, waiting for client`);
        // No auto-reply — wait for VAD or client input

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

app.get('/health', (_, res) => res.json({ status: 'ok', v: '2.2' }));
app.get('/', (_, res) => res.json({ name: 'Twilio + Grok Voice Bridge', v: '2.2' }));

server.listen(PORT, () => {
  console.log(`\n🚀 Twilio + Grok Bridge v2.2 on :${PORT}`);
  console.log(`🔊 Serial audio queue | 160 bytes/chunk | 20ms pacing`);
});

module.exports = { app, server };