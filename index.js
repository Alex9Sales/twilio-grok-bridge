/**
 * Twilio + Grok Voice Bridge v2.1
 * Audio streaming bidirecional: Twilio ←→ Grok Voice
 *
 * Grok → Twilio (outbound):
 *   PCM16 LE 24kHz (base64) → downsample 3:1 → PCM16 8kHz → μ-law 8kHz
 *   Envio em chunks de 160 bytes (20ms) com pacing ~20ms entre chunks
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

// ─── μ-law encoding (G.711) ──────────────────────────────────────────────────

const ULAW_MAP = new Int16Array(256);
(function buildUlawMap() {
  const BIAS = 33;
  for (let i = 0; i < 256; i++) {
    let val = i ^ 0x55;
    let sign = (val & 0x80) ? -1 : 1;
    let exponent = (val >> 4) & 0x07;
    let mantissa = val & 0x0F;
    let step = (4 << exponent);
    let adj = mantissa * step / 16 + step;
    if (exponent > 0) adj += 4;
    ULAW_MAP[i] = sign * adj;
  }
})();

function pcm16ToUlaw(pcmBuf) {
  // pcmBuf: Buffer of raw PCM16 LE samples
  const out = Buffer.alloc(Math.floor(pcmBuf.length / 2));
  for (let i = 0; i < pcmBuf.length - 1; i += 2) {
    const sample = pcmBuf.readInt16LE(i);
    const abs = Math.abs(sample);
    let exp = 0;
    let mask = 0x2000;
    while (abs > mask && exp < 15) { mask >>= 1; exp++; }
    let mantissa = exp === 0 ? abs >> 4 : abs >> (exp + 3);
    if (mantissa > 15) mantissa = 15;
    let ulawbyte = (exp << 4) | mantissa;
    ulawbyte = sample < 0 ? 0x80 | (0x7F - ulawbyte) : 0x80 | ulawbyte;
    out[Math.floor(i / 2)] = ulawbyte;
  }
  return out;
}

// ─── AUDIO CONVERTER (PCM24 → μ-law 8kHz) ────────────────────────────────────

function convertAndChunkAudio(base64Audio) {
  // Decode base64 → raw PCM buffer (Grok pode retornar PCM16 LE ou PCM24)
  const raw = Buffer.from(base64Audio, 'base64');
  console.log('[AUDIO] input bytes:', raw.length);

  // Detectar formato: se length % 3 === 0 → possivelmente PCM24 (3 bytes/sample)
  // se length % 2 === 0 → PCM16 LE (2 bytes/sample)
  let pcm8k;
  if (raw.length % 3 === 0) {
    console.log('[AUDIO] Detected: PCM24 (3 bytes/sample)');
    // PCM24 → PCM16 8kHz
    const numSamples24 = Math.floor(raw.length / 3);
    const samples24 = new Int16Array(numSamples24);
    for (let i = 0; i < numSamples24; i++) {
      // PCM24: 3 bytes little-endian, signed
      const b0 = raw[i * 3];
      const b1 = raw[i * 3 + 1];
      const b2 = raw[i * 3 + 2];
      let s24 = b0 | (b1 << 8) | ((b2 << 16) & 0xFF0000);
      if (s24 & 0x800000) s24 |= ~0xFFFFFF;
      samples24[i] = s24 >> 8; // PCM24 → PCM16 com shift
    }
    // Downsample 24kHz → 8kHz (every 3rd)
    const numSamples8 = Math.floor(samples24.length / 3);
    const samples8 = new Int16Array(numSamples8);
    for (let i = 0; i < numSamples8; i++) samples8[i] = samples24[i * 3];
    // Build PCM16 LE buffer
    pcm8k = Buffer.alloc(numSamples8 * 2);
    for (let i = 0; i < numSamples8; i++) pcm8k.writeInt16LE(samples8[i], i * 2);

  } else if (raw.length % 2 === 0) {
    console.log('[AUDIO] Detected: PCM16 LE (2 bytes/sample)');
    // PCM16 24kHz LE → PCM16 8kHz
    const numSamples24 = Math.floor(raw.length / 2);
    const samples24 = new Int16Array(numSamples24);
    for (let i = 0; i < numSamples24; i++) samples24[i] = raw.readInt16LE(i * 2);
    // Downsample 24kHz → 8kHz
    const numSamples8 = Math.floor(samples24.length / 3);
    const samples8 = new Int16Array(numSamples8);
    for (let i = 0; i < numSamples8; i++) samples8[i] = samples24[i * 3];
    // Build PCM16 LE buffer
    pcm8k = Buffer.alloc(numSamples8 * 2);
    for (let i = 0; i < numSamples8; i++) pcm8k.writeInt16LE(samples8[i], i * 2);

  } else {
    console.log('[AUDIO] Unknown format, trying PCM16');
    pcm8k = Buffer.alloc(raw.length);
    pcm8k = raw;
  }

  console.log('[AUDIO] PCM16 8kHz samples:', Math.floor(pcm8k.length / 2));
  const ulaw = pcm16ToUlaw(pcm8k);
  console.log('[AUDIO] μ-law 8kHz bytes:', ulaw.length);

  // Chunk into 160-byte packets (20ms each at 8kHz)
  const CHUNK = 160;
  const chunks = [];
  for (let i = 0; i < ulaw.length; i += CHUNK) {
    chunks.push(ulaw.slice(i, i + CHUNK));
  }
  console.log('[AUDIO] Chunks:', chunks.length, 'x 160 bytes');
  return chunks;
}

// ─── TWILIO WEBHOOK ────────────────────────────────────────────────────────────

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
  let callSid = null;
  let streamSid = null;
  let customerPhone = 'unknown';
  let customerName = 'Cliente';
  let grokWs = null;
  let isGrokSessionReady = false;
  let responseCount = 0;

  console.log('[WS] Twilio connected');

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      const event = data.event;

      if (event === 'start') {
        streamSid = data.start?.streamSid;
        callSid = data.start?.callSid;
        customerPhone = data.start?.parameters?.From || 'unknown';
        customerName = data.start?.customParameters?.customerName || 'Cliente';
        console.log('[WS] Stream start — streamSid:', streamSid, '| callSid:', callSid);
        console.log('[WS] Customer:', customerName, customerPhone);
        console.log('[WS] Connecting to Grok...');
        connectToGrokVoice();

      } else if (event === 'media') {
        // Twilio → Grok
        const payload = data.media?.payload;
        if (payload && grokWs?.readyState === WebSocket.OPEN && isGrokSessionReady) {
          grokWs.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: payload }));
        }

      } else if (event === 'mark') {
        console.log('[WS] Mark:', data.mark?.name);

      } else if (event === 'stop') {
        console.log('[WS] Stream stopped — reason:', data.stop?.reason);
        if (grokWs) { grokWs.close(); grokWs = null; }
        isGrokSessionReady = false;

      } else if (event === 'dtmf') {
        console.log('[WS] DTMF:', data.dtmf?.digit);
      }
    } catch (e) {
      console.error('[WS] Error:', e.message);
    }
  });

  ws.on('close', () => {
    console.log('[WS] Twilio disconnected');
    if (grokWs) { grokWs.close(); grokWs = null; }
    isGrokSessionReady = false;
  });

  ws.on('error', (err) => console.error('[WS] Error:', err.message));

  // ─── Grok Voice Connection ─────────────────────────────────────────────────

  function connectToGrokVoice() {
    grokWs = new WebSocket(
      'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0',
      { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
    );

    grokWs.on('open', () => {
      console.log('[GROK] Connected');
      const prompt = `Você é a assistente virtual da Família do Gás, empresa de delivery de gás em Campo Grande, MS. Você está telefonando para um cliente inativo. Haja de forma natural, amigável, como uma pessoa real — nunca diga que é IA. Use a voz Sal, neutra. Máx 2 minutos. Se o cliente não atender, deixe mensagem breve e desligue.`;

      grokWs.send(JSON.stringify({
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
      console.log('[GROK] Session configured (Sal, PCM24 output)');

      // Trigger greeting
      grokWs.send(JSON.stringify({
        type: 'conversation.item.create',
        item: {
          type: 'message', role: 'user',
          content: [{ type: 'input_text', text: 'Olá, estou ligando da Família do Gás. Tem um minuto?' }]
        }
      }));
      grokWs.send(JSON.stringify({ type: 'response.create' }));
      console.log('[GROK] Greeting sent');
    });

    grokWs.on('message', (msg) => {
      try {
        const evt = JSON.parse(msg);
        const t = evt.type || '';

        if (t === 'session.updated') {
          isGrokSessionReady = true;
          console.log('[GROK] Session ready');

        } else if (t === 'response.output_audio.delta') {
          // Áudio Grok → converter e enviar para Twilio com pacing
          const audioBase64 = evt.delta;
          if (!audioBase64) { console.log('[GROK] empty audio delta'); return; }
          console.log('[GROK] audio delta received, len:', audioBase64.length);

          const chunks = convertAndChunkAudio(audioBase64);
          console.log('[TWILIO] sending', chunks.length, 'chunks, 160 bytes each, 20ms pacing');

          // Send chunks with ~20ms delay between each
          let chunkIndex = 0;
          const sendNext = () => {
            if (chunkIndex >= chunks.length) {
              console.log('[TWILIO] all chunks sent');
              return;
            }
            if (ws.readyState === WebSocket.OPEN && streamSid) {
              ws.send(JSON.stringify({
                event: 'media',
                streamSid: streamSid,
                media: { payload: chunks[chunkIndex].toString('base64') }
              }));
              console.log(`[TWILIO] chunk ${chunkIndex + 1}/${chunks.length} sent, ${chunks[chunkIndex].length} bytes`);
            }
            chunkIndex++;
            setTimeout(sendNext, 20);
          };
          sendNext();

        } else if (t === 'response.output_audio_transcript.delta') {
          process.stdout.write(evt.delta);

        } else if (t === 'response.done') {
          responseCount++;
          console.log(`\n[CALL] Response complete (${responseCount}) — keeping call open, waiting for client`);
          // Don't auto-reply — just wait for VAD or client input

        } else if (t === 'response.function_call_arguments.done') {
          const name = evt.name;
          const args = JSON.parse(evt.arguments || '{}');
          console.log(`[TOOL] ${name}:`, args);
          if (name === 'register_result') {
            sendResultToN8N(callSid, customerPhone, customerName, args.intent, args.notes || '');
          }
          grokWs.send(JSON.stringify({
            type: 'conversation.item.create',
            item: { type: 'function_call_output', call_id: evt.call_id, output: JSON.stringify({ success: true }) }
          }));
          grokWs.send(JSON.stringify({ type: 'response.create' }));

        } else if (t === 'input_audio_buffer.speech_started') {
          console.log('[GROK] VAD triggered — client speaking, cancelling AI');
          if (grokWs?.readyState === WebSocket.OPEN) grokWs.send(JSON.stringify({ type: 'response.cancel' }));

        } else if (t === 'conversation.item.input_audio_transcription.completed') {
          console.log('[GROK] Client said:', evt.transcript);

        } else if (t === 'error') {
          console.error('[GROK] Error:', evt.error?.message);
        }
      } catch (e) {
        console.error('[GROK] Parse error:', e.message);
      }
    });

    grokWs.on('close', () => {
      console.log('[GROK] Closed');
      isGrokSessionReady = false;
    });

    grokWs.on('error', (err) => console.error('[GROK] Error:', err.message));
  }
});

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

app.get('/health', (_, res) => res.json({ status: 'ok', v: '2.1' }));
app.get('/', (_, res) => res.json({ name: 'Twilio + Grok Voice Bridge', v: '2.1' }));

server.listen(PORT, () => {
  console.log(`\n🚀 Twilio + Grok Bridge v2.1 on :${PORT}`);
  console.log(`🔊 Audio: PCM24/PCM16 → μ-law 8kHz | chunks: 160bytes/20ms pacing`);
});

module.exports = { app, server };