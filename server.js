require('dotenv').config();
const express = require('express');
const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    fetchLatestBaileysVersion,
    Browsers,
    makeCacheableSignalKeyStore,
    makeInMemoryStore
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const cron = require('node-cron');
const fs = require('fs');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const http = require('http');
const https = require('https');

const httpAgent = new http.Agent({ keepAlive: true, maxSockets: Infinity, maxFreeSockets: 1024, timeout: 30000, scheduling: 'lifo' });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: Infinity, maxFreeSockets: 1024, timeout: 30000, scheduling: 'lifo' });

class SecurityShield {
    static protect(req, res, next) {
        res.setHeader('X-Powered-By', 'Quantum-Core-Engine-v10-Ultra');
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.setHeader('X-Frame-Options', 'DENY');
        res.setHeader('X-XSS-Protection', '1; mode=block');
        res.setHeader('Strict-Transport-Security', 'max-age=63072000; includeSubDomains; preload');
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
        res.removeHeader('Server');
        next();
    }
}

class CircuitBreaker {
    constructor(failureThreshold = 3, resetTimeout = 20000) {
        this.failureThreshold = failureThreshold;
        this.resetTimeout = resetTimeout;
        this.failures = 0;
        this.state = 'CLOSED';
        this.nextAttempt = Date.now();
        this.lock = false;
    }
    async execute(action) {
        if (this.state === 'OPEN') {
            if (Date.now() > this.nextAttempt) {
                this.state = 'HALF_OPEN';
            } else {
                throw new Error('CIRCUIT_OPEN');
            }
        }
        while (this.lock) await new Promise(r => setImmediate(r));
        this.lock = true;
        try {
            const result = await action();
            this.reset();
            this.lock = false;
            return result;
        } catch (error) {
            this.recordFailure();
            this.lock = false;
            throw error;
        }
    }
    recordFailure() {
        this.failures += 1;
        if (this.failures >= this.failureThreshold) {
            this.state = 'OPEN';
            this.nextAttempt = Date.now() + this.resetTimeout;
        }
    }
    reset() {
        this.failures = 0;
        this.state = 'CLOSED';
    }
}

class RetryStrategy {
    static async execute(fn, maxRetries = 10, baseDelay = 200) {
        let attempt = 0;
        while (attempt < maxRetries) {
            try {
                return await fn();
            } catch (error) {
                attempt++;
                if (attempt >= maxRetries) throw error;
                const jitter = crypto.randomInt(50, 300);
                const delay = Math.min(baseDelay * Math.pow(1.5, attempt) + jitter, 10000);
                await new Promise(res => setTimeout(res, delay));
            }
        }
    }
}

class AdvancedJobScheduler {
    constructor() {
        this.jobs = new Map();
        this.metrics = { executed: 0, failed: 0, active: 0 };
    }
    schedule(cronExpression, timezone, task, jobId) {
        if (this.jobs.has(jobId)) {
            this.jobs.get(jobId).job.stop();
            this.jobs.delete(jobId);
        }
        const wrappedTask = async () => {
            try {
                await task();
                this.metrics.executed++;
            } catch (error) {
                this.metrics.failed++;
            }
        };
        const job = cron.schedule(cronExpression, wrappedTask, { scheduled: true, timezone });
        this.jobs.set(jobId, { job, expression: cronExpression, createdAt: Date.now() });
        this.metrics.active = this.jobs.size;
        return jobId;
    }
    cleanup() {
        for (const [id, jobData] of this.jobs.entries()) {
            jobData.job.stop();
            this.jobs.delete(id);
        }
        this.metrics.active = 0;
    }
}

class WhatsAppConnectionManager extends EventEmitter {
    constructor() {
        super();
        this.sock = null;
        this.isConnecting = false;
        this.reconnectTimer = null;
        this.reconnectAttempts = 0;
        this.msgRetryCounterCache = new Map();
        this.logger = pino({ level: 'silent' });
        this.circuitBreaker = new CircuitBreaker(5, 15000);
        this.connectionLock = false;
        this.store = makeInMemoryStore({ logger: this.logger });
    }
    
    async getValidSocket() {
        if (!this.sock || !this.sock.user) {
            await this.initialize();
        }
        let waitTime = 0;
        while (this.isConnecting && waitTime < 45000) {
            await new Promise(r => setTimeout(r, 50));
            waitTime += 50;
        }
        if (!this.sock) throw new Error('SOCKET_UNAVAILABLE');
        return this.sock;
    }

    async waitForOpenConnection() {
        let waitTime = 0;
        while ((!this.sock || !this.sock.user) && waitTime < 60000) {
            await new Promise(r => setTimeout(r, 200));
            waitTime += 200;
        }
        if (!this.sock || !this.sock.user) throw new Error('CONNECTION_TIMEOUT');
        return this.sock;
    }

    async destroySocket() {
        if (this.sock) {
            try {
                this.sock.ev.removeAllListeners();
                if (this.sock.ws) {
                    this.sock.ws.removeAllListeners();
                    if (this.sock.ws.readyState === 1) this.sock.ws.close();
                }
                if (typeof this.sock.end === 'function') this.sock.end();
            } catch (e) {}
            this.sock = null;
        }
    }

    async initialize() {
        while (this.connectionLock) await new Promise(r => setTimeout(r, 10));
        if (this.isConnecting) return;
        this.connectionLock = true;
        this.isConnecting = true;
        
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }

        try {
            await this.destroySocket();
            const { state, saveCreds } = await useMultiFileAuthState('./whatsapp_session');
            const { version } = await fetchLatestBaileysVersion();
            
            this.sock = makeWASocket({
                version,
                auth: {
                    creds: state.creds,
                    keys: makeCacheableSignalKeyStore(state.keys, this.logger),
                },
                logger: this.logger,
                printQRInTerminal: false,
                browser: Browsers.macOS('Desktop'),
                generateHighQualityLinkPreview: true,
                syncFullHistory: false,
                markOnlineOnConnect: true,
                connectTimeoutMs: 60000,
                defaultQueryTimeoutMs: 60000,
                keepAliveIntervalMs: 20000,
                retryRequestDelayMs: 250,
                maxMsgRetryCount: 25,
                msgRetryCounterCache: this.msgRetryCounterCache,
                agent: httpsAgent,
                getMessage: async () => ({ conversation: 'whatsapp-auto-call' })
            });

            this.store.bind(this.sock.ev);
            this.sock.ev.on('creds.update', saveCreds);
            
            this.sock.ev.on('connection.update', async (update) => {
                const { connection, lastDisconnect } = update;
                if (connection === 'close') {
                    const statusCode = lastDisconnect?.error?.output?.statusCode;
                    const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                    if (shouldReconnect) {
                        this.reconnectAttempts++;
                        const delay = Math.min(500 * Math.pow(1.2, this.reconnectAttempts) + crypto.randomInt(100, 300), 10000);
                        if (!this.reconnectTimer) {
                            this.reconnectTimer = setTimeout(async () => {
                                this.reconnectTimer = null;
                                await this.initialize();
                            }, delay);
                        }
                    } else {
                        if (fs.existsSync('./whatsapp_session')) {
                            fs.rmSync('./whatsapp_session', { recursive: true, force: true });
                        }
                        this.reconnectAttempts = 0;
                        if (!this.reconnectTimer) {
                            this.reconnectTimer = setTimeout(async () => {
                                this.reconnectTimer = null;
                                await this.initialize();
                            }, 1000);
                        }
                    }
                } else if (connection === 'open') {
                    this.reconnectAttempts = 0;
                    this.emit('connected', this.sock);
                }
            });
        } catch (err) {
            if (!this.reconnectTimer) {
                this.reconnectTimer = setTimeout(async () => {
                    this.reconnectTimer = null;
                    await this.initialize();
                }, 3000);
            }
        } finally {
            this.isConnecting = false;
            this.connectionLock = false;
        }
    }
}

const app = express();
const waManager = new WhatsAppConnectionManager();
const jobScheduler = new AdvancedJobScheduler();

app.use(SecurityShield.protect);
app.use(express.json({ limit: '50mb', strict: false }));
app.use(express.urlencoded({ extended: true, limit: '50mb', parameterLimit: 100000 }));
app.use(express.static('public', { maxAge: '1d' }));

app.post('/api/request-code', async (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ error: 'الرجاء إدخال رقم الهاتف' });
    
    try {
        const socket = await waManager.getValidSocket();
        await waManager.waitForOpenConnection();
        
        console.log({
            phoneNumber,
            user: socket.user,
            connected: !!socket.user
        });

        const executeRequest = async () => {
            let code = await socket.requestPairingCode(phoneNumber);
            return code?.match(/.{1,4}/g)?.join('-') || code;
        };
        
        const code = await waManager.circuitBreaker.execute(() => RetryStrategy.execute(executeRequest, 7, 500));
        res.json({ success: true, code: code });
    } catch (error) {
        console.error(error);
        res.status(500).json({ 
            error: error.message,
            stack: error.stack 
        });
    }
});

app.post('/api/schedule-call', (req, res) => {
    const { targetNumber, time } = req.body;
    if (!targetNumber || !time) {
        return res.status(400).json({ error: 'الرجاء إدخال جميع البيانات' });
    }

    const [hour, minute] = time.split(':');
    const cronExpression = `${minute} ${hour} * * *`;
    const jobId = crypto.createHash('sha3-512').update(`${targetNumber}-${time}-${Date.now()}-${crypto.randomUUID()}`).digest('hex');

    const executionTask = async () => {
        try {
            const socket = await waManager.getValidSocket();
            await waManager.waitForOpenConnection();
            
            const jid = targetNumber.includes('@s.whatsapp.net') ? targetNumber : `${targetNumber}@s.whatsapp.net`;
            const makeCallRequest = async () => {
                await socket.query({
                    tag: 'iq',
                    attrs: { to: jid, type: 'set', xmlns: 'w:g2' },
                    content: [{ tag: 'call', attrs: { action: 'init' } }]
                });
            };
            await RetryStrategy.execute(makeCallRequest, 35, 800);
        } catch (e) {}
    };

    jobScheduler.schedule(cronExpression, "Africa/Cairo", executionTask, jobId);
    res.json({ success: true, message: `تمت جدولة الاتصال بنجاح بتوقيت القاهرة الساعة ${time}` });
});

const gracefulShutdown = async () => {
    jobScheduler.cleanup();
    await waManager.destroySocket();
    process.exit(0);
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);
process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});

waManager.initialize().catch(() => {});

const PORT = process.env.PORT || 3000;
const server = http.createServer(app);

server.keepAliveTimeout = 65000;
server.headersTimeout = 70000;
server.maxConnections = 100000;

server.listen(PORT, '0.0.0.0', () => {});
