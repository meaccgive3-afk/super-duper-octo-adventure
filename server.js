require('dotenv').config();
const express = require('express');
const { default: makeWASocket, useMultiFileAuthState } = require('@whiskeysockets/baileys');
const pino = require('pino');
const cron = require('node-cron');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static('public')); // يقرأ ملف index.html من مجلد public

let sock;
let authState;

// 1. طلب كود الربط (Pairing Code)
app.post('/api/request-code', async (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ error: 'الرجاء إدخال رقم الهاتف' });

    try {
        // تهيئة بيئة حفظ الجلسة لمرة واحدة
        authState = await useMultiFileAuthState('./whatsapp_session');
        
        sock = makeWASocket({
            auth: authState.state,
            logger: pino({ level: 'silent' }),
            browser: ["Ubuntu", "Chrome", "20.0.04"]
        });

        // حفظ التحديثات تلقائياً لملفات الجلسة
        sock.ev.on('creds.update', authState.saveCreds);

        // طلب الكود من سيرفرات واتساب
        setTimeout(async () => {
            try {
                let code = await sock.requestPairingCode(phoneNumber);
                code = code?.match(/.{1,4}/g)?.join('-') || code;
                res.json({ success: true, code: code });
            } catch (err) {
                res.status(500).json({ error: 'فشل طلب الكود، تأكد من الرقم أو حاول لاحقاً' });
            }
        }, 3000);

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 2. جدولة المكالمة
app.post('/api/schedule-call', (req, res) => {
    const { targetNumber, time } = req.body;
    
    if (!targetNumber || !time) {
        return res.status(400).json({ error: 'الرجاء إدخال جميع البيانات' });
    }

    const [hour, minute] = time.split(':');
    const cronExpression = `${minute} ${hour} * * *`;

    // جدولة المهمة بتوقيت القاهرة المتأثر بملف .env أو الاستضافة
    cron.schedule(cronExpression, async () => {
        console.log(`بدء الاتصال التلقائي على الرقم: ${targetNumber}`);
        
        const jid = `${targetNumber}@s.whatsapp.net`;
        let calling = true;

        while (calling) {
            try {
                // إرسال طلب بدء الاتصال
                await sock.query({
                    tag: 'iq',
                    attrs: { to: jid, type: 'set', xmlns: 'w:g2' },
                    content: [{ tag: 'call', attrs: { action: 'init' } }]
                });
                
                // الانتظار 30 ثانية قبل المحاولة التالية إذا لم يتم الرد
                await new Promise(r => setTimeout(r, 30000));
            } catch (e) {
                console.log("تم رفض المكالمة أو حدث خطأ، إعادة المحاولة بعد 5 ثوانٍ...");
                await new Promise(r => setTimeout(r, 5000));
            }
        }
    }, {
        scheduled: true,
        timezone: "Africa/Cairo"
    });

    res.json({ success: true, message: `تمت جدولة الاتصال بنجاح بتوقيت القاهرة الساعة ${time}` });
});

// تشغيل السيرفر على المنفذ المحدد في .env أو 3000 كافتراضي
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`السيرفر يعمل بنجاح على المنفذ ${PORT}`));
