/**
 * Twilio + Grok Voice Bridge — outbound calling agent
 * Recebe chamadas do Twilio → conecta ao Grok Voice WebSocket → streaming bidirecional
 */

require('dotenv').config();
const express = require('express');
const { VoiceResponse } = require('twilio').twiml;
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

let activeCallSessions = new Map();

// ─── TWILIO WEBHOOK (inbound call handler) ─────────────────────────────────────

app.post('/voice', (req, res) => {
  console.log('[TWILIO] POST /voice — incoming call');
  console.log('[TWILIO] CallSid:', req.body?.CallSid);
  console.log('[TWILIO] From:', req.body?.From);
  console.log('[TWILIO] To:', req.body?.To);

  // Use <Connect><Stream> para agentes de voz em tempo real com WebSocket bidirecional
  // Não usar <Start><Stream> — é o protocolo antigo
  const twiML = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://twilio.salestecnologia.com.br/grok-media-stream"/>
  </Connect>
</Response>`;

  console.log('[TWILIO] Sending TwiML with Connect+Stream');
  res.type('text/xml').send(twiML);
});

app.post('/voice/call-status', (req, res) => {
  console.log('[TWILIO] Call status:', req.body.CallStatus, 'Sid:', req.body.CallSid);
  res.sendStatus(200);
});

// ─── WEBSOCKET SERVER (para Twilio Media Stream) ──────────────────────────────

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', (ws, req) => {
  console.log('[WS] Twilio media stream connected');
  let callSid = null;
  let grokWs = null;
  let isGrokSessionReady = false;

  console.log('[WS] Twilio media stream connected');
  console.log('[WS] Waiting for Twilio stream start event...');

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      const event = data.event;

      if (event === 'start') {
        callSid = data.start.callSid;
        const customerPhone = data.start.parameters?.From || 'unknown';
        const customerName = data.start.customParameters?.customerName || 'Cliente';

        console.log(`[CALL] Stream started: ${callSid}`);
        console.log(`[CALL] From: ${customerPhone}`);
        console.log(`[CALL] Customer: ${customerName}`);
        console.log('[CALL] Connecting to Grok Voice API...');

        const session = { callSid, customerPhone, customerName, ws, grokWs: null, isGrokSessionReady: false };
        activeCallSessions.set(callSid, session);
        connectToGrokVoice(callSid, customerPhone, customerName);

      } else if (event === 'media') {
        // Áudio do Twilio (cliente) → enviar para Grok Voice
        const audioPayload = data.media.payload;
        const audioSize = audioPayload ? audioPayload.length : 0;

        if (grokWs && grokWs.readyState === WebSocket.OPEN && isGrokSessionReady) {
          grokWs.send(JSON.stringify({
            type: 'input_audio_buffer.append',
            audio: audioPayload
          }));
        } else {
          console.log(`[WS] Grok not ready — buffering audio (${audioSize} bytes)`);
        }

      } else if (event === 'dtmf') {
        console.log(`[WS] DTMF: ${data.dtmf?.digit || '?'}`);

      } else if (event === 'stop') {
        console.log(`[CALL] Stream ended: ${callSid}`);
        console.log(`[CALL] Reason: ${data.stop?.reason || 'unknown'}`);
        if (grokWs) {
          console.log('[CALL] Closing Grok connection...');
          grokWs.close();
          grokWs = null;
        }
        activeCallSessions.delete(callSid);

      } else if (event === 'mark') {
        console.log(`[WS] Mark: ${data.mark?.name || '?'}`);

      } else {
        console.log(`[WS] Event: ${event}`);
      }
    } catch (e) {
      console.error('[WS] Error processing message:', e.message);
    }
  });

  ws.on('close', () => {
    if (grokWs) grokWs.close();
    if (callSid) activeCallSessions.delete(callSid);
  });

  ws.on('error', (err) => {
    console.error('[WS] Error:', err.message);
  });

  function connectToGrokVoice(callSid, customerPhone, customerName) {
    const grokWsUrl = 'wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0';
    
    grokWs = new WebSocket(grokWsUrl, {
      headers: { Authorization: `Bearer ${XAI_API_KEY}` }
    });

    grokWs.on('open', () => {
      console.log('[GROK] Connected to Grok Voice');
      
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
      console.log('[GROK] Session configured');
    });

    grokWs.on('message', (msg) => {
      try {
        const event = JSON.parse(msg);
        const etype = event.type || '';

        if (etype === 'session.updated') {
          isGrokSessionReady = true;
          console.log('[GROK] Session ready — streaming audio');
          
          grokWs.send(JSON.stringify({
            type: 'conversation.item.create',
            item: {
              type: 'message',
              role: 'user',
              content: [{ type: 'input_text', text: 'Inicie a ligação agora.' }]
            }
          }));
          grokWs.send(JSON.stringify({ type: 'response.create' }));

        } else if (etype === 'response.output_audio.delta') {
          const audioChunk = event.delta;
          if (ws.readyState === WebSocket.OPEN && audioChunk) {
            ws.send(JSON.stringify({
              event: 'media',
              media: { payload: audioChunk }
            }));
          }

        } else if (etype === 'response.output_audio_transcript.delta') {
          process.stdout.write(event.delta);

        } else if (etype === 'response.done') {
          console.log('\n[GROK] Response complete');

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
        }
      } catch (e) {
        console.error('[GROK] Parse error:', e.message);
      }
    });

    grokWs.on('close', () => {
      console.log('[GROK] WebSocket closed');
      isGrokSessionReady = false;
      if (ws.readyState === WebSocket.OPEN) ws.close();
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
      sid: c.sid,
      from: c.from,
      to: c.to,
      status: c.status,
      duration: c.duration,
      startTime: c.startTime
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

// ─── HEALTH CHECK ─────────────────────────────────────────────────────────────

app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    activeSessions: activeCallSessions.size,
    timestamp: new Date().toISOString()
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Twilio + Grok Voice Bridge',
    version: '1.0.0',
    endpoints: {
      'POST /voice': 'Twilio webhook for inbound calls',
      'POST /call': 'Initiate outbound call {to, customerName, customerId}',
      'GET /calls': 'List recent calls',
      'GET /health': 'Health check'
    }
  });
});

// ─── START ────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log(`\n🚀 Twilio + Grok Voice Bridge running on port ${PORT}`);
  console.log(`📞 Twilio number: ${TWILIO_PHONE_NUMBER}`);
  console.log(`🔑 Grok Voice model: grok-voice-think-fast-1.0`);
});

module.exports = { app, server };