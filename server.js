require('dotenv').config();
const express = require('express');
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, Browsers } = require('@whiskeysockets/baileys');
const pino = require('pino');
const cron = require('node-cron');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static('public'));

let sock;
const logger = pino({ level: 'error' });

process.on('uncaughtException', (err) => console.error(err));
process.on('unhandledRejection', (err) => console.error(err));

async function initializeWhatsApp() {
    try {
        const { state, saveCreds } = await useMultiFileAuthState('./whatsapp_session');
        const { version } = await fetchLatestBaileysVersion();

        sock = makeWASocket({
            version,
            auth: state,
            logger,
            printQRInTerminal: false,
            browser: Browsers.macOS('Chrome'),
            generateHighQualityLinkPreview: true,
            syncFullHistory: false,
            markOnlineOnConnect: true,
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: 60000,
            keepAliveIntervalMs: 10000,
            getMessage: async () => { return { conversation: 'whatsapp-auto-call' }; }
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            if (connection === 'close') {
                const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
                console.error(`Connection closed. Reconnecting: ${shouldReconnect}`, lastDisconnect?.error);
                if (shouldReconnect) {
                    setTimeout(initializeWhatsApp, 3000);
                } else {
                    if (fs.existsSync('./whatsapp_session')) {
                        fs.rmSync('./whatsapp_session', { recursive: true, force: true });
                    }
                    setTimeout(initializeWhatsApp, 3000);
                }
            } else if (connection === 'open') {
                console.log('WhatsApp connection securely opened.');
            }
        });

        sock.ev.on('error', (err) => console.error('Baileys Error:', err));

        return sock;
    } catch (err) {
        console.error('Initialization Error:', err);
    }
}

app.post('/api/request-code', async (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ error: 'الرجاء إدخال رقم الهاتف' });

    try {
        if (!sock || !sock.user) {
            await initializeWhatsApp();
        }

        setTimeout(async () => {
            try {
                let code = await sock.requestPairingCode(phoneNumber);
                code = code?.match(/.{1,4}/g)?.join('-') || code;
                res.json({ success: true, code: code });
            } catch (err) {
                console.error('Pairing Code Error:', err);
                res.status(500).json({ error: 'فشل طلب الكود، تأكد من الرقم أو حاول لاحقاً' });
            }
        }, 3000);

    } catch (error) {
        console.error('API Request Error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/schedule-call', (req, res) => {
    const { targetNumber, time } = req.body;
    
    if (!targetNumber || !time) {
        return res.status(400).json({ error: 'الرجاء إدخال جميع البيانات' });
    }

    const [hour, minute] = time.split(':');
    const cronExpression = `${minute} ${hour} * * *`;

    cron.schedule(cronExpression, async () => {
        try {
            if (!sock || !sock.user) await initializeWhatsApp();
            
            const jid = `${targetNumber}@s.whatsapp.net`;
            let calling = true;

            while (calling) {
                try {
                    await sock.query({
                        tag: 'iq',
                        attrs: { to: jid, type: 'set', xmlns: 'w:g2' },
                        content: [{ tag: 'call', attrs: { action: 'init' } }]
                    });
                    
                    await new Promise(r => setTimeout(r, 30000));
                } catch (e) {
                    console.error('Call Query Error:', e);
                    await new Promise(r => setTimeout(r, 5000));
                }
            }
        } catch (cronErr) {
            console.error('Cron Execution Error:', cronErr);
        }
    }, {
        scheduled: true,
        timezone: "Africa/Cairo"
    });

    res.json({ success: true, message: `تمت جدولة الاتصال بنجاح بتوقيت القاهرة الساعة ${time}` });
});

initializeWhatsApp().catch(err => console.error('Startup Error:', err));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`السيرفر يعمل بنجاح على المنفذ ${PORT}`));
