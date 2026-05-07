/**
 * Twilio + Grok Voice Bridge v2.10
 * Fix: higher RMS threshold, echo guard, suppress stale deltas, correct silence timer
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
  AUDIO_SOURCE_RATE = '24000',
  AUDIO_GAIN = '0.85',
  MAX_QUEUE_SIZE = '300',
  LOCAL_VAD_ENABLED = 'true',
  BARGE_IN_RMS_THRESHOLD = '180',
  BARGE_IN_MIN_FRAMES = '4',
  BARGE_IN_ECHO_GUARD_MS = '500'
} = process.env;

const sourceRate = parseInt(AUDIO_SOURCE_RATE) || 24000;
const downsampleFactor = sourceRate >= 24000 ? 3 : 2;
const gain = parseFloat(AUDIO_GAIN) || 0.85;
const MAX_QUEUE = parseInt(MAX_QUEUE_SIZE) || 300;
const PREBUFFER = 3;
const SILENCE_TIMEOUT = 5000;
const VAD_ENABLED = LOCAL_VAD_ENABLED === 'true';
const RMS_THRESH = parseInt(BARGE_IN_RMS_THRESHOLD) || 180;
const MIN_FRAMES = parseInt(BARGE_IN_MIN_FRAMES) || 4;
const ECHO_GUARD = parseInt(BARGE_IN_ECHO_GUARD_MS) || 500;

console.log(`[CONFIG] rate:${sourceRate} gain:${gain} maxQ:${MAX_QUEUE}`);
console.log(`[LOCAL_VAD] enabled:${VAD_ENABLED} rms:${RMS_THRESH} minFrames:${MIN_FRAMES} echoGuard:${ECHO_GUARD}ms`);

// ─── UTILITIES ───────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── μ-law decode (for VAD) ─────────────────────────────────────────────────

function ulawToPcm16(byte) {
  byte = ~byte;
  const sign = byte & 0x80 ? -1 : 1;
  const e = (byte >> 4) & 0x07;
  const m = byte & 0x0F;
  const step = (4 << e);
  const adj = e === 0 ? (m * step / 16 + step) : (m * step / 16 + step + 4);
  return sign * adj;
}

function calcRms(ulawBuf) {
  let sum = 0;
  for (let i = 0; i < ulawBuf.length; i++) {
    const s = ulawToPcm16(ulawBuf[i]);
    sum += s * s;
  }
  return Math.sqrt(sum / ulawBuf.length);
}

// ─── G.711 μ-law encode ─────────────────────────────────────────────────────

const BIAS = 0x84, CLIP = 32635;
function toUlaw(s) {
  let g = s * gain;
  if (g > 32767) g = 32767; if (g < -32768) g = -32768;
  let sign = g < 0 ? 0x80 : 0;
  if (sign) g = -g;
  if (g > CLIP) g = CLIP;
  g += BIAS;
  let e = 7;
  for (let m = 0x4000; (g & m) === 0 && e > 0; e--, m >>= 1) {}
  return ~(sign | (e << 4) | ((g >> (e + 3)) & 0x0F)) & 0xFF;
}

function pcmToUlaw(buf) {
  const out = Buffer.alloc(Math.floor(buf.length / 2));
  for (let i = 0; i < out.length; i++) out[i] = toUlaw(buf.readInt16LE(i * 2));
  return out;
}

// ─── 3-sample avg downsample ────────────────────────────────────────────────

function downsample(samples, factor) {
  const out = new Int16Array(Math.floor(samples.length / factor));
  for (let i = 0; i < out.length; i++) {
    let sum = 0;
    for (let j = 0; j < factor; j++) sum += samples[i * factor + j];
    out[i] = Math.round(sum / factor);
  }
  return out;
}

// ─── AUDIO CONVERTER ────────────────────────────────────────────────────────

function toChunks(base64) {
  const raw = Buffer.from(base64, 'base64');
  const n = Math.floor(raw.length / 2);
  const s = new Int16Array(n);
  for (let i = 0; i < n; i++) s[i] = raw.readInt16LE(i * 2);
  const out = downsample(s, downsampleFactor);
  const pcm = Buffer.alloc(out.length * 2);
  for (let i = 0; i < out.length; i++) pcm.writeInt16LE(out[i], i * 2);
  const ulaw = pcmToUlaw(pcm);
  const chunks = [];
  for (let i = 0; i < ulaw.length; i += 160) chunks.push(ulaw.slice(i, i + 160));
  return chunks;
}

// ─── CALL STATE ────────────────────────────────────────────────────────────

function makeState(ws, sid, callSid, phone, name) {
  return {
    ws, sid, callSid, phone, name,
    queue: [],
    sending: false,
    preCount: 0,
    grok: null,
    grokReady: false,
    callActive: true,
    userSpeaking: false,
    aiSpeaking: false,
    suppressAiAudio: false,
    playbackGen: 0,
    responseGen: 0,
    grokRespActive: false,
    silenceTimer: null,
    silenceAttempts: 0,
    // Local VAD
    vadFrameCount: 0,
    echoGuardUntil: 0, // timestamp until which VAD is suppressed
    lastRms: 0
  };
}

// ─── BARGE-IN ──────────────────────────────────────────────────────────────

function doBargeIn(st) {
  console.log('[BARGE_IN] user started speaking');
  st.userSpeaking = true;
  st.aiSpeaking = false;
  st.suppressAiAudio = true;

  // Clear Twilio
  if (st.ws?.readyState === WebSocket.OPEN && st.sid) {
    st.ws.send(JSON.stringify({ event: 'clear', streamSid: st.sid }));
    console.log('[BARGE_IN] Twilio clear sent');
  }

  st.queue = [];
  console.log('[AUDIO_QUEUE] cleared due to barge-in');

  // Cancel Grok response
  if (st.grokRespActive && st.grok?.readyState === WebSocket.OPEN) {
    st.grok.send(JSON.stringify({ type: 'response.cancel' }));
    st.grokRespActive = false;
    console.log('[GROK] response cancelled due to barge-in');
  }

  // Mark this generation as cancelled
  st.playbackGen++;
  console.log('[BARGE_IN] gen incremented to', st.playbackGen);

  if (st.silenceTimer) { clearTimeout(st.silenceTimer); st.silenceTimer = null; }
}

// ─── LOCAL VAD ────────────────────────────────────────────────────────────

function checkLocalVad(st, ulawPayload) {
  if (!VAD_ENABLED) return;
  if (st.echoGuardUntil > Date.now()) {
    console.log('[BARGE_IN] echo guard active, ignoring local VAD');
    st.vadFrameCount = 0;
    return;
  }
  if (!st.aiSpeaking && st.queue.length === 0) {
    st.vadFrameCount = 0;
    return;
  }
  if (st.suppressAiAudio) {
    st.vadFrameCount = 0;
    return;
  }

  const rms = calcRms(ulawPayload);
  st.lastRms = Math.round(rms);
  if (rms >= RMS_THRESH) {
    st.vadFrameCount++;
    console.log('[LOCAL_VAD] rms:', st.lastRms, 'threshold:', RMS_THRESH, 'frames:', st.vadFrameCount, '/', MIN_FRAMES);
    if (st.vadFrameCount >= MIN_FRAMES && !st.userSpeaking) {
      st.vadFrameCount = 0;
      doBargeIn(st);
    }
  } else {
    st.vadFrameCount = 0;
  }
}

// ─── SILENCE TIMER ────────────────────────────────────────────────────────

function startSilenceTimer(st) {
  if (st.silenceTimer) clearTimeout(st.silenceTimer);
  // Only start when: no AI speaking, queue empty, client not speaking, call active
  if (st.aiSpeaking || st.queue.length > 0 || st.userSpeaking || !st.callActive) return;

  console.log('[SILENCE] timer started (ai done, queue empty, client quiet)');
  st.silenceTimer = setTimeout(() => {
    if (!st.callActive || st.userSpeaking || st.aiSpeaking || st.queue.length > 0) return;
    st.silenceAttempts++;
    console.log('[SILENCE] timeout, attempt', st.silenceAttempts);

    if (st.silenceAttempts <= 1) {
      if (st.grok?.readyState === WebSocket.OPEN) {
        st.grok.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Cliente não respondeu. Diga: "Alô, consegue me ouvir?"' }] } }));
        st.grok.send(JSON.stringify({ type: 'response.create' }));
      }
    } else {
      console.log('[SILENCE] max attempts, ending politely');
      if (st.grok?.readyState === WebSocket.OPEN) {
        st.grok.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Encerrar. Diga apenas: "Tudo bem, bom dia!"' }] } }));
        st.grok.send(JSON.stringify({ type: 'response.create' }));
      }
    }
  }, SILENCE_TIMEOUT);
}

// ─── AUDIO SENDER ───────────────────────────────────────────────────────

async function sendQueue(st) {
  if (st.sending || st.queue.length === 0 || st.userSpeaking) return;
  st.sending = true;
  const gen = st.playbackGen;

  // Prebuffer
  st.preCount = 0;
  while (st.preCount < PREBUFFER && st.queue.length > 0 && !st.userSpeaking && st.playbackGen === gen) {
    await sleep(20);
    st.preCount++;
  }
  if (st.userSpeaking || st.playbackGen !== gen) {
    st.sending = false;
    console.log('[AUDIO] aborted prebuffer');
    return;
  }

  console.log('[AUDIO] prebuffer done, sending', st.queue.length, 'chunks');

  while (st.queue.length > 0) {
    if (!st.callActive || st.userSpeaking || st.playbackGen !== gen) {
      console.log('[AUDIO_QUEUE] stopped due to', !st.callActive ? 'call end' : 'barge-in');
      break;
    }
    const chunk = st.queue.shift();
    if (st.ws.readyState === WebSocket.OPEN && st.sid) {
      st.ws.send(JSON.stringify({ event: 'media', streamSid: st.sid, media: { payload: chunk.toString('base64') } }));
    }
    await sleep(20);
  }

  st.sending = false;
  console.log('[AUDIO] done sending');

  if (st.queue.length === 0 && !st.userSpeaking && !st.aiSpeaking) {
    startSilenceTimer(st);
  }
}

// ─── TWILIO WEBHOOKS ────────────────────────────────────────────────────

app.post('/voice', (req, res) => {
  console.log('[TWILIO]', req.body?.CallSid);
  res.type('text/xml').send(`<?xml version="1.0" encoding="UTF-8"?><Response><Connect><Stream url="wss://twilio.salestecnologia.com.br/grok-media-stream"/></Connect></Response>`);
});

app.post('/voice-say', (_, res) => {
  res.type('text/xml').send(`<?xml version="1.0" encoding="UTF-8"?><Response><Say language="pt-BR" voice="alice">Teste.</Say></Response>`);
});

// ─── WEBSOCKET ─────────────────────────────────────────────────────────

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (ws, req) => {
  let st = null;

  ws.on('message', msg => {
    try {
      const d = JSON.parse(msg);
      if (d.event === 'start') {
        st = makeState(ws, d.start?.streamSid, d.start?.callSid, d.start?.parameters?.From || '?', d.start?.customParameters?.customerName || 'Client');
        console.log('[WS] start', st.sid);
        connectGrok(st);

      } else if (d.event === 'media' && st) {
        // To Grok
        if (d.media?.payload && st.grok?.readyState === WebSocket.OPEN && st.grokReady) {
          st.grok.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: d.media.payload }));
        }
        // Local VAD
        if (VAD_ENABLED && d.media?.payload) {
          const ulawBytes = Buffer.from(d.media.payload, 'base64');
          checkLocalVad(st, ulawBytes);
        }

      } else if (d.event === 'stop' && st) {
        console.log('[WS] stop');
        st.callActive = false;
        st.queue = [];
        st.userSpeaking = false;
        if (st.silenceTimer) clearTimeout(st.silenceTimer);
        if (st.grok) { st.grok.close(); st.grok = null; }
      }
    } catch (e) { console.error('[WS]', e.message); }
  });

  ws.on('close', () => {
    console.log('[WS] disconnect');
    if (st) { st.callActive = false; st.queue = []; }
  });
});

// ─── GROK ────────────────────────────────────────────────────────────────

function connectGrok(st) {
  st.grok = new WebSocket(
    'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0',
    { headers: { Authorization: `Bearer ${XAI_API_KEY}` } }
  );

  st.grok.on('open', () => {
    console.log('[GROK] connected');

    const persona = `Você é a assistente virtual da Família do Gás, delivery de gás em Campo Grande, MS. Liga para clientes inativos há mais de 15 dias.

FALE UMA FRASE. Aguarde resposta. Não emende frases extras.

ABERTURA: "Olá, aqui é da Família do Gás. Está precisando de gás hoje?"

register_result(intent, notes) — intent: venda | interesse | agendar | sem_interesse | sem_resposta`;

    st.grok.send(JSON.stringify({
      type: 'session.update',
      session: {
        voice: 'Sal',
        instructions: persona,
        turn_detection: { type: 'server_vad', threshold: 0.5, silence_duration_ms: 500, prefix_padding_ms: 200 },
        tools: [{ type: 'function', name: 'register_result', description: 'registra',
          parameters: { type: 'object', properties: { intent: { type: 'string', enum: ['venda','interesse','agendar','sem_interesse','sem_resposta'] }, notes: { type: 'string' } }, required: ['intent'] } }],
        input_audio_transcription: { model: 'grok-2-audio' },
        audio: { input: { format: { type: 'audio/pcm', rate: 24000 } }, output: { format: { type: 'audio/pcm', rate: 24000 } } }
      }
    }));

    st.grok.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Ligue.' }] } }));
    st.grok.send(JSON.stringify({ type: 'response.create' }));
    console.log('[GROK] greeting sent');
  });

  st.grok.on('message', msg => {
    try {
      const e = JSON.parse(msg);
      const t = e.type || '';

      if (t === 'session.updated') {
        st.grokReady = true;

      } else if (t === 'response.output_audio.delta') {
        // Discard if suppressed (cancelled response)
        if (st.suppressAiAudio) {
          console.log('[AUDIO] discarded stale/cancelled audio delta');
          return;
        }
        if (!e.delta || !st.callActive) return;

        st.aiSpeaking = true;
        st.grokRespActive = true;
        st.userSpeaking = false;
        st.vadFrameCount = 0;

        // Enable echo guard — suppress local VAD while AI speaks
        st.echoGuardUntil = Date.now() + ECHO_GUARD;

        const chunks = toChunks(e.delta);
        st.queue.push(...chunks);
        console.log('[AUDIO] queued', chunks.length, '| queue:', st.queue.length, '| gen:', st.playbackGen);

        if (st.queue.length > MAX_QUEUE) {
          console.log('[AUDIO] WARNING queue at', st.queue.length);
        }

        sendQueue(st);

      } else if (t === 'response.output_audio_transcript.delta') {
        process.stdout.write(e.delta);

      } else if (t === 'response.done') {
        st.grokRespActive = false;
        st.aiSpeaking = false;
        console.log('[GROK] response done');
        startSilenceTimer(st);

      } else if (t === 'input_audio_buffer.speech_started') {
        console.log('[GROK_VAD] client speech started');
        if (st.grokRespActive && st.grok?.readyState === WebSocket.OPEN) {
          st.grok.send(JSON.stringify({ type: 'response.cancel' }));
          st.grokRespActive = false;
          console.log('[GROK] response cancelled by client VAD');
        }
        doBargeIn(st);

      } else if (t === 'input_audio_buffer.speech_stopped') {
        console.log('[BARGE_IN] client stopped speaking');
        st.userSpeaking = false;
        st.silenceAttempts = 0;

      } else if (t === 'response.function_call_arguments.done') {
        const args = JSON.parse(e.arguments || '{}');
        console.log('[TOOL]', e.name, args);
        if (e.name === 'register_result') sendResult(st.callSid, st.phone, st.customerName, args.intent, args.notes || '');
        st.grok.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'function_call_output', call_id: e.call_id, output: JSON.stringify({ success: true }) } }));
        st.grok.send(JSON.stringify({ type: 'response.create' }));

      } else if (t === 'conversation.item.input_audio_transcription.completed') {
        console.log('[GROK] client said:', e.transcript);
        st.userSpeaking = false;

      } else if (t === 'error') {
        console.error('[GROK]', e.error?.message);
      }
    } catch (err) { console.error('[GROK] parse', err.message); }
  });

  st.grok.on('close', () => { st.grokReady = false; st.grokRespActive = false; console.log('[GROK] close'); });
  st.grok.on('error', err => console.error('[GROK]', err.message));
}

server.on('upgrade', (req, socket, head) => {
  if (req.url === '/grok-media-stream') wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  else socket.destroy();
});

// ─── OUTBOUND + N8N + HEALTH ─────────────────────────────────────────────

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
  res.json({ calls: (await c.calls.list({ limit: 20 })).map(x => ({ sid: x.sid, from: x.from, to: x.to, status: x.status, duration: x.duration })) });
});

async function sendResult(callSid, phone, name, intent, notes) {
  if (!N8N_WEBHOOK_URL) return;
  try {
    const r = await fetch(N8N_WEBHOOK_URL, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source: 'twilio_grok', call_sid: callSid, customer_name: name, phone, intent, notes }) });
    console.log('[N8N]', r.ok);
  } catch (e) { console.error('[N8N]', e.message); }
}

app.get('/health', (_, r) => r.json({ status: 'ok', v: '2.10', gain, maxQueue: MAX_QUEUE, localVad: VAD_ENABLED, rmsThresh: RMS_THRESH, echoGuard: ECHO_GUARD }));
app.get('/', (_, r) => r.json({ name: 'Twilio + Grok', v: '2.10' }));

server.listen(PORT, () => console.log(`\n🚀 v2.10 | rms:${RMS_THRESH} echoGuard:${ECHO_GUARD}ms | queue:${MAX_QUEUE}`));

module.exports = { app, server };