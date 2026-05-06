/**
 * Twilio + Grok Voice Bridge — outbound calling agent
 * Recebe chamadas do Twilio → conecta ao Grok Voice WebSocket → streaming bidirecional
 *
 * Formato de áudio:
 * - Grok Voice: PCM16 24kHz (base64)
 * - Twilio Media Streams: g711_ulaw 8000Hz (base64)
 * - Conversão: PCM16 24kHz → resample 8kHz → PCM16 8kHz → μ-law
 */

require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const http = require('http');

const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const {
  TWILIO_ACCOUNT_SID,
  TWILIO_AUTH_TOKEN,
  TWILIO_PHONE_NUMBER,
  XAI_API_KEY,
  N8N_WEBHOOK_URL,
  PORT = 3000
} = process.env;

// ─── PCM16 24kHz → μ-law 8kHz CONVERTER ───────────────────────────────────────

/**
 * Converte PCM16 24kHz buffer para μ-law 8kHz
 * O Twilio Media Streams espera audio g711_ulaw 8000Hz
 * @param {Buffer} pcm24kHz - PCM16 little-endian 24kHz audio
 * @returns {Buffer} - μ-law encoded 8kHz audio
 */
function convertPcm24ToMulaw8(pcm24kHz) {
  // Step 1: Resample 24kHz → 8kHz (take every 3rd sample)
  const pcm8kHz = [];
  for (let i = 0; i < pcm24kHz.length - 1; i += 3) {
    pcm8kHz.push(pcm24kHz[i] | (pcm24kHz[i + 1] << 8));
  }

  // Step 2: Convert PCM16 → μ-law
  const mulaw = Buffer.alloc(pcm8kHz.length);

  // μ-law encoding table
  const ULAW_TABLE = [
    -32124, -31100, -30108, -29156, -28236, -27348, -26490, -25662,
    -24862, -24090, -23346, -22630, -21942, -21280, -20646, -20038,
    -19456, -18899, -18367, -17859, -17374, -16913, -16474, -16056,
    -15660, -15284, -14928, -14590, -14272, -13971, -13686, -13418,
    -13166, -12929, -12706, -12497, -12301, -12118, -11947, -11788,
    -11640, -11503, -11375, -11256, -11144, -11040, -10943, -10852,
    -10766, -10686, -10610, -10538, -10471, -10408, -10348, -10291,
    -10237, -10186, -10138, -10092, -10048, -10007, -9967, -9929,
    -9893, -9858, -9825, -9793, -9762, -9732, -9703, -9675,
    -9648, -9622, -9597, -9573, -9549, -9526, -9504, -9482,
    -9461, -9440, -9420, -9401, -9382, -9363, -9345, -9327,
    -9309, -9292, -9275, -9258, -9242, -9226, -9211, -9195,
    -9180, -9165, -9151, -9136, -9122, -9109, -9095, -9082,
    -9069, -9056, -9043, -9031, -9018, -9006, -8994, -8983,
    -8971, -8960, -8948, -8937, -8926, -8916, -8905, -8894,
    -8884, -8874, -8864, -8854, -8844, -8834, -8825, -8815,
    -8806, -8797, -8788, -8779, -8770, -8761, -8753, -8744,
    -8736, -8727, -8719, -8711, -8703, -8695, -8687, -8679,
    -8672, -8664, -8656, -8649, -8641, -8634, -8627, -8620,
    -8612, -8605, -8598, -8591, -8585, -8578, -8571, -8565,
    -8558, -8552, -8545, -8539, -8533, -8527, -8521, -8515,
    -8509, -8503, -8497, -8491, -8485, -8480, -8474, -8469,
    -8463, -8458, -8452, -8447, -8441, -8436, -8431, -8426,
    -8421, -8416, -8411, -8406, -8401, -8396, -8391, -8386,
    -8382, -8377, -8372, -8368, -8363, -8359, -8354, -8350,
    -8345, -8341, -8337, -8332, -8328, -8324, -8320, -8316,
    -8312, -8308, -8304, -8300, -8296, -8292, -8288, -8284,
    -8280, -8277, -8273, -8269, -8266, -8262, -8258, -8255,
    -8251, -8248, -8244, -8241, -8237, -8234, -8230, -8227,
    -8224, -8220, -8217, -8214, -8211, -8207, -8204, -8201,
    -8198, -8195, -8192, -8189, -8186, -8183, -8180, -8177,
  ];

  for (let i = 0; i < pcm8kHz.length; i++) {
    const sample = Math.max(-32768, Math.min(32767, pcm8kHz[i]));
    const abs = Math.abs(sample);
    let ulawbyte;

    if (abs >= 32000) {
      ulawbyte = 0x7F;
    } else {
      // Find the μ-law table index
      let idx = 0;
      for (let t = 0; t < ULAW_TABLE.length; t++) {
        if (abs >= Math.abs(ULAW_TABLE[t])) {
          idx = t;
          break;
        }
      }
      ulawbyte = idx;
      if (sample > 0) ulawbyte = 0x80 | (0x7F - ulawbyte);
      else ulawbyte = 0x80 | ulawbyte;
    }
    mulaw[i] = ulawbyte;
  }

  return mulaw;
}

/**
 * Decodifica base64 audio PCM16 24kHz da Grok, converte para μ-law 8kHz
 * e retorna base64 para enviar à Twilio
 */
function grokAudioToTwilio(base64Pcm24) {
  try {
    const pcmBuf = Buffer.from(base64Pcm24, 'base64');
    // Converter para Int16 little-endian
    const int16 = new Int16Array(pcmBuf.length / 2);
    for (let i = 0; i < int16.length; i++) {
      int16[i] = pcmBuf[i * 2] | (pcmBuf[i * 2 + 1] << 8);
    }
    // Converter para Buffer PCM16 8kHz (downsampling)
    const pcm8 = Buffer.alloc(int16.length * 2);
    for (let i = 0; i < int16.length; i++) {
      // Simple downsampling: take every 3rd sample
      const srcIdx = Math.floor(i * 3);
      if (srcIdx < int16.length) {
        pcm8[i * 2] = int16[srcIdx] & 0xFF;
        pcm8[i * 2 + 1] = (int16[srcIdx] >> 8) & 0xFF;
      }
    }
    // Agora converter PCM16 8kHz → μ-law
    const mulaw = convertPcm16ToMulaw(Buffer.from(pcm8));
    return mulaw.toString('base64');
  } catch (e) {
    console.error('[CONVERT] Error:', e.message);
    return null;
  }
}

/**
 * Converte PCM16 samples para μ-law
 */
function convertPcm16ToMulaw(pcmBuf) {
  const ULAW_MAP = [];
  for (let i = 0; i < 256; i++) ULAW_MAP[i] = 0;

  const BIAS = 33;
  const MULAW_MAX = 32768;

  // Build μ-law table
  for (let i = 0; i < 128; i++) {
    const VL = 0x100 | (i << 1);
    const sign = 0;
    const exponent = (VL >> 8) & 0x0F;
    const mantissa = VL & 0x0F;
    const step = (4 << exponent);
    let SEG = exponent * 16;
    let adj = 0;
    if (mantissa !== 0) {
      adj = step + (mantissa * step / 16);
      SEG |= 0x10;
    }
    if (sign !== 0) adj = -adj;
    ULAW_MAP[128 + i] = (i === 127 ? 254 : adj);
    ULAW_MAP[128 - i] = (i === 127 ? 254 : -adj);
  }

  const out = Buffer.alloc(pcmBuf.length / 2);
  for (let i = 0; i < pcmBuf.length - 1; i += 2) {
    const sample = pcmBuf[i] | (pcmBuf[i + 1] << 8);
    let abs = Math.abs(sample) + BIAS;
    let raw = abs > MULAW_MAX ? MULAW_MAX - BIAS : abs - BIAS;
    let ulawbyte = 0;

    if (raw >= 256) {
      let tmp = raw;
      let exp = 7;
      while (exp > 0 && (tmp > 255)) {
        tmp >>= 1;
        exp--;
      }
      raw >>= exp;
      ulawbyte = (exp << 4) | (raw & 0x0F);
    } else {
      ulawbyte = raw >> 4;
    }
    ulawbyte = (sample < 0 ? 0x80 : 0) | (ulawbyte ^ 0x0D);
    out[i / 2] = ulawbyte;
  }
  return out;
}

// ─── GROK AUDIO → TWILIO CONVERTER (simplificado) ─────────────────────────────

/**
 * Converte base64 PCM16 da Grok (24kHz) para base64 μ-law (8kHz)
 * Retorna null se falhar
 */
function convertGrokAudioToTwilioFormat(base64Audio) {
  try {
    // Decode base64 → Buffer PCM16 24kHz
    const pcmBuffer = Buffer.from(base64Audio, 'base64');

    // Extrair Int16 samples
    const samples24k = new Int16Array(pcmBuffer.length / 2);
    for (let i = 0; i < samples24k.length; i++) {
      samples24k[i] = pcmBuffer.readInt16LE(i * 2);
    }

    // Downsample 24kHz → 8kHz (every 3rd sample)
    const samples8k = [];
    for (let i = 0; i < samples24k.length; i += 3) {
      samples8k.push(samples24k[i]);
    }

    // Create PCM16 8kHz buffer
    const pcm8Buffer = Buffer.alloc(samples8k.length * 2);
    for (let i = 0; i < samples8k.length; i++) {
      pcm8Buffer.writeInt16LE(samples8k[i], i * 2);
    }

    // Encode PCM16 8kHz → μ-law
    const ulawBuffer = Buffer.alloc(samples8k.length);
    for (let i = 0; i < samples8k.length; i++) {
      const sample = samples8k[i];
      const abs = Math.abs(sample);
      let exponent = 0;
      let mask = 0x2000;

      while (abs > mask && exponent < 15) {
        mask >>= 1;
        exponent++;
      }

      const mantissa = (abs >> (exponent === 0 ? 4 : exponent + 3)) & 0x0F;
      let ulawbyte = (exponent << 4) | mantissa;
      if (sample < 0) ulawbyte = 0x80 | (0x7F - ulawbyte);
      else ulawbyte = 0x80 | ulawbyte;

      ulawBuffer[i] = ulawbyte;
    }

    return ulawBuffer.toString('base64');
  } catch (e) {
    console.error('[CONVERT] Conversion error:', e.message);
    return null;
  }
}

// ─── TWILIO WEBHOOK ────────────────────────────────────────────────────────────

app.post('/voice', (req, res) => {
  console.log('[TWILIO] POST /voice — incoming call');
  console.log('[TWILIO] CallSid:', req.body?.CallSid, '| From:', req.body?.From, '| To:', req.body?.To);

  // Usar <Connect><Stream> para agente de voz em tempo real com WebSocket bidirecional
  const twiML = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://twilio.salestecnologia.com.br/grok-media-stream"/>
  </Connect>
</Response>`;

  console.log('[TWILIO] Sending TwiML: <Connect><Stream>');
  res.type('text/xml').send(twiML);
});

app.post('/voice/call-status', (req, res) => {
  console.log('[TWILIO] Call status:', req.body?.CallStatus, 'Sid:', req.body?.CallSid);
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

  console.log('[WS] Twilio media stream connected');

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      const event = data.event;

      if (event === 'start') {
        // Início do stream — Twilio indica os IDs
        streamSid = data.start?.streamSid;
        callSid = data.start?.callSid;
        customerPhone = data.start?.parameters?.From || 'unknown';
        customerName = data.start?.customParameters?.customerName || 'Cliente';

        console.log('[WS] Stream started');
        console.log('[WS] streamSid:', streamSid);
        console.log('[WS] callSid:', callSid);
        console.log('[WS] From:', customerPhone, '| Name:', customerName);
        console.log('[WS] Connecting to Grok Voice API...');

        connectToGrokVoice();

      } else if (event === 'media') {
        // Áudio do cliente Twilio → Grok Voice
        const audioPayload = data.media?.payload;
        if (audioPayload && grokWs && grokWs.readyState === WebSocket.OPEN && isGrokSessionReady) {
          grokWs.send(JSON.stringify({
            type: 'input_audio_buffer.append',
            audio: audioPayload
          }));
        } else if (!grokWs || !isGrokSessionReady) {
          console.log('[WS] Audio received but Grok not ready — buffering');
        }

      } else if (event === 'mark') {
        console.log('[WS] Mark received:', data.mark?.name);

      } else if (event === 'stop') {
        console.log('[WS] Stream stopped. Reason:', data.stop?.reason);
        console.log('[WS] Closing Grok connection...');
        if (grokWs) {
          grokWs.close();
          grokWs = null;
        }
        isGrokSessionReady = false;

      } else if (event === 'dtmf') {
        console.log('[WS] DTMF digit:', data.dtmf?.digit);

      }
    } catch (e) {
      console.error('[WS] Error:', e.message);
    }
  });

  ws.on('close', () => {
    console.log('[WS] Twilio disconnected');
    if (grokWs) {
      grokWs.close();
      grokWs = null;
    }
    isGrokSessionReady = false;
  });

  ws.on('error', (err) => {
    console.error('[WS] Error:', err.message);
  });

  // ─── Grok Voice Connection ─────────────────────────────────────────────────

  function connectToGrokVoice() {
    const grokWsUrl = 'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0';

    grokWs = new WebSocket(grokWsUrl, {
      headers: { Authorization: `Bearer ${XAI_API_KEY}` }
    });

    grokWs.on('open', () => {
      console.log('[GROK] Connected to Grok Voice WebSocket');

      const agentInstructions = `Você é a assistente virtual da Família do Gás, empresa de delivery de gás em Campo Grande, MS.

## CONTEXTO
Você está telefonando para um cliente que está inativo há mais de 15 dias.
O objetivo é oferecer o botijão de gás com desconto especial de 5% no próximo pedido.

## CHAMADA ATUAL
Cliente: ${customerName}
Telefone: ${customerPhone}

## INSTRUÇÕES
- Fale de forma natural, clara e amigável como uma pessoa real
- NUNCA diga que é uma IA ou robô
- A voz deve ser neutra, sem sotaque brasileiro marcado
- Máximo de 2 minutos por chamada
- Seja educada mesmo se o cliente recusar

## ROTEIRO

### ABERTURA:
"Olá, tudo bem? Aqui é da Família do Gás, delivery de gás em Campo Grande, MS. Estou ligando para uma pesquisa rápida sobre nossos serviços. Tem um minutinho?"

### SE ACEITAR:
"Ótimo! Muito obrigado. primeiramente, gostaríamos de saber: está tudo bem com você? Ainda utiliza gás de botijão em casa?"

### OFERTA (se cliente usa gás):
"Temos uma oferta especial para clientes antigos. Fizemos uma parceria e você pode ganhar 5% de desconto no próximo pedido de gás, entrega em até 2 horas em qualquer região de Campo Grande. Quer Aproveitar?"

### CAPTURA INTERESSE:
- Se disser que sim → registrar "interesse"
- Se quiser agendar → registrar "agendar"
- Se não tiver interesse → registrar "sem_interesse"
- Se não atender → registrar "sem_resposta"

### ENCERRAMENTO (sempre):
"Se tiver interesse, é só chamar no WhatsApp: 67 9361-8055. Muito obrigado pela atenção! Tenha um excelente dia!"

## TOOLS
Você pode usar a ferramenta register_result para registar o resultado da chamada no sistema.

register_result(intent, notes):
- intent: "venda" | "interesse" | "agendar" | "sem_interesse" | "sem_resposta"
- notes: notas adicionais sobre a conversa
`;

      grokWs.send(JSON.stringify({
        type: 'session.update',
        session: {
          voice: 'Sal',
          instructions: agentInstructions,
          turn_detection: { type: 'server_vad', threshold: 0.85, silence_duration_ms: 0 },
          tools: [{
            type: 'function',
            name: 'register_result',
            description: 'Registra o resultado da chamada no sistema',
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
      console.log('[GROK] Session configured (voice=Sal, 24kHz PCM output)');

      // Trigger opening message
      grokWs.send(JSON.stringify({
        type: 'conversation.item.create',
        item: {
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: 'Inicie a ligação agora.' }]
        }
      }));
      grokWs.send(JSON.stringify({ type: 'response.create' }));
      console.log('[GROK] Opening message sent');
    });

    grokWs.on('message', (msg) => {
      try {
        const event = JSON.parse(msg);
        const etype = event.type || '';

        if (etype === 'session.updated') {
          isGrokSessionReady = true;
          console.log('[GROK] Session ready');

        } else if (etype === 'response.output_audio.delta') {
          // Áudio recebido da Grok Voice — converter e enviar para Twilio
          const audioBase64 = event.delta;
          if (!audioBase64) {
            console.log('[GROK] Audio delta received (empty)');
            return;
          }

          console.log('[GROK] Audio delta received, length:', audioBase64.length);

          // Converter PCM24 → μ-law 8kHz
          const twilioAudio = convertGrokAudioToTwilioFormat(audioBase64);

          if (!twilioAudio) {
            console.log('[GROK] Audio conversion failed — sending raw PCM');
            // Fallback: enviar raw
            if (ws.readyState === WebSocket.OPEN && streamSid) {
              ws.send(JSON.stringify({
                event: 'media',
                streamSid: streamSid,
                media: { payload: audioBase64 }
              }));
            }
            return;
          }

          // Enviar para Twilio
          if (ws.readyState === WebSocket.OPEN && streamSid) {
            ws.send(JSON.stringify({
              event: 'media',
              streamSid: streamSid,
              media: { payload: twilioAudio }
            }));
            console.log('[TWILIO] Audio sent, bytes:', twilioAudio.length);
          }

        } else if (etype === 'response.output_audio_transcript.delta') {
          process.stdout.write(event.delta);

        } else if (etype === 'response.done') {
          // Fim da resposta da IA — MANTÉM A CHAMADA ABERTA
          console.log('\n[CALL] Response complete — keeping call open (waiting for client)');

          // Não fechar Grok nem Twilio — aguardar próximo input do cliente
          // Optionally trigger re-prompt
          if (grokWs && grokWs.readyState === WebSocket.OPEN) {
            setTimeout(() => {
              if (grokWs.readyState === WebSocket.OPEN && ws.readyState === WebSocket.OPEN) {
                grokWs.send(JSON.stringify({
                  type: 'conversation.item.create',
                  item: {
                    type: 'message',
                    role: 'user',
                    content: [{ type: 'input_text', text: 'O cliente está a ouvir. Aguarda a resposta dele.' }]
                  }
                }));
                grokWs.send(JSON.stringify({ type: 'response.create' }));
                console.log('[GROK] Prompt sent to continue conversation');
              }
            }, 1000);
          }

        } else if (etype === 'response.function_call_arguments.done') {
          const name = event.name;
          const args = JSON.parse(event.arguments || '{}');
          console.log(`[TOOL] ${name}:`, args);

          if (name === 'register_result') {
            sendResultToN8N(callSid, customerPhone, customerName, args.intent, args.notes || '');
          }

          grokWs.send(JSON.stringify({
            type: 'conversation.item.create',
            item: {
              type: 'function_call_output',
              call_id: event.call_id,
              output: JSON.stringify({ success: true, registered: true })
            }
          }));
          grokWs.send(JSON.stringify({ type: 'response.create' }));

        } else if (etype === 'error') {
          console.error('[GROK] Error:', event.error?.message);

        } else if (etype === 'input_audio_buffer.speech_started') {
          console.log('[GROK] Client started speaking — VAD triggered');
          // Optionally cancel AI response
          if (grokWs && grokWs.readyState === WebSocket.OPEN) {
            grokWs.send(JSON.stringify({ type: 'response.cancel' }));
            console.log('[GROK] Response cancelled due to client speech');
          }

        } else if (etype === 'conversation.item.input_audio_transcription.completed') {
          console.log('[GROK] Client transcript:', event.transcript);
        }
      } catch (e) {
        console.error('[GROK] Parse error:', e.message);
      }
    });

    grokWs.on('close', () => {
      console.log('[GROK] WebSocket closed');
      isGrokSessionReady = false;
    });

    grokWs.on('error', (err) => {
      console.error('[GROK] Error:', err.message);
    });
  }
});

server.on('upgrade', (req, socket, head) => {
  if (req.url === '/grok-media-stream') {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

// ─── OUTBOUND CALL API ────────────────────────────────────────────────────────

app.post('/call', async (req, res) => {
  const { to, customerName, customerId } = req.body;

  if (!to) {
    return res.status(400).json({ error: 'Missing "to" phone number' });
  }

  try {
    const client = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    const call = await client.calls.create({
      to,
      from: TWILIO_PHONE_NUMBER,
      url: `https://${req.headers.host}/voice`,
      statusCallback: `https://${req.headers.host}/voice/call-status`,
      statusCallbackEvent: ['completed', 'no-answer', 'busy'],
      customerName: customerName || 'Cliente',
      customerId: customerId || null
    });

    console.log(`[OUTBOUND] Call initiated: ${call.sid} → ${to}`);
    res.json({ success: true, callSid: call.sid, status: call.status });
  } catch (err) {
    console.error('[OUTBOUND] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/calls', async (req, res) => {
  try {
    const client = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    const calls = await client.calls.list({ limit: 20 });
    res.json({ calls: calls.map(c => ({
      sid: c.sid, from: c.from, to: c.to,
      status: c.status, duration: c.duration, startTime: c.startTime
    }))});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── N8N WEBHOOK ──────────────────────────────────────────────────────────────

async function sendResultToN8N(callSid, phone, name, intent, notes) {
  if (!N8N_WEBHOOK_URL) return;
  try {
    const response = await fetch(N8N_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source: 'twilio_grok_outbound',
        call_sid: callSid,
        customer_name: name,
        phone,
        intent,
        notes,
        timestamp: new Date().toISOString()
      })
    });
    console.log('[N8N] Result sent:', response.ok);
  } catch (e) {
    console.error('[N8N] Error:', e.message);
  }
}

// ─── HEALTH CHECK ────────────────────────────────────────────────────────────

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    activeSessions: 0,
    timestamp: new Date().toISOString()
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Twilio + Grok Voice Bridge',
    version: '2.0',
    audio: 'PCM24 → μ-law 8kHz conversion',
    endpoints: {
      'POST /voice': 'Twilio webhook',
      'POST /call': 'Initiate outbound call',
      'GET /calls': 'List recent calls',
      'GET /health': 'Health check'
    }
  });
});

// ─── START ────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`\n🚀 Twilio + Grok Voice Bridge v2.0 on port ${PORT}`);
  console.log(`📞 Twilio number: ${TWILIO_PHONE_NUMBER}`);
  console.log(`🔑 Grok Voice: grok-voice-think-fast-1.0 | voice: Sal`);
  console.log(`🔊 Audio: PCM24 → μ-law 8kHz (Twilio compatible)`);
});

module.exports = { app, server };