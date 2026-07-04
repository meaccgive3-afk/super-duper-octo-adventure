require('dotenv').config();
const express = require('express');
const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    fetchLatestBaileysVersion,
    Browsers,
    makeCacheableSignalKeyStore
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const cron = require('node-cron');
const fs = require('fs');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const http = require('http');
const https = require('https');
const zlib = require('zlib');
const os = require('os');

const agentOptions = {
    keepAlive: true,
    keepAliveMsecs: 10000,
    maxSockets: Infinity,
    maxFreeSockets: 2048,
    timeout: 60000,
    scheduling: 'lifo'
};
const httpAgent = new http.Agent(agentOptions);
const httpsAgent = new https.Agent(agentOptions);

class AsyncMutex {
    constructor() {
        this.queue = [];
        this.locked = false;
    }
    async acquire() {
        if (!this.locked) {
            this.locked = true;
            return;
        }
        return new Promise(resolve => this.queue.push(resolve));
    }
    release() {
        if (this.queue.length > 0) {
            const next = this.queue.shift();
            setImmediate(next);
        } else {
            this.locked = false;
        }
    }
}

class MemoryGuardian {
    static start(interval = 60000, thresholdMB = 1024) {
        setInterval(() => {
            const usage = process.memoryUsage();
            const rssMB = Math.round(usage.rss / 1024 / 1024);
            if (rssMB > thresholdMB) {
                if (global.gc) {
                    global.gc();
                }
            }
        }, interval).unref();
    }
}

class AdvancedRateLimiter {
    constructor(windowMs, maxRequests) {
        this.windowMs = windowMs;
        this.maxRequests = maxRequests;
        this.clients = new Map();
        setInterval(() => this.clients.clear(), this.windowMs).unref();
    }
    middleware() {
        return (req, res, next) => {
            const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
            const current = this.clients.get(ip) || 0;
            if (current >= this.maxRequests) {
                return res.status(429).json({ error: 'Too Many Requests' });
            }
            this.clients.set(ip, current + 1);
            next();
        };
    }
}

class QuantumSecurity {
    static protect(req, res, next) {
        res.setHeader('X-Powered-By', 'Quantum-Nexus-Core-v11-Ultra');
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.setHeader('X-Frame-Options', 'DENY');
        res.setHeader('X-XSS-Protection', '1; mode=block');
        res.setHeader('Strict-Transport-Security', 'max-age=63072000; includeSubDomains; preload');
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
        res.removeHeader('Server');
        if (req.method === 'OPTIONS') {
            return res.sendStatus(204);
        }
        next();
    }
    static compress(req, res, next) {
        const acceptEncoding = req.headers['accept-encoding'] || '';
        if (!acceptEncoding.match(/\b(gzip|deflate)\b/)) return next();
        const rawSend = res.send;
        res.send = function (body) {
            if (typeof body === 'string' || Buffer.isBuffer(body)) {
                res.setHeader('Content-Encoding', acceptEncoding.includes('gzip') ? 'gzip' : 'deflate');
                const compressed = acceptEncoding.includes('gzip') ? zlib.gzipSync(body) : zlib.deflateSync(body);
                return rawSend.call(this, compressed);
            }
            return rawSend.call(this, body);
        };
        next();
    }
}

class CircuitBreaker {
    constructor(failureThreshold = 3, resetTimeout = 15000) {
        this.failureThreshold = failureThreshold;
        this.resetTimeout = resetTimeout;
        this.failures = 0;
        this.state = 'CLOSED';
        this.nextAttempt = Date.now();
        this.mutex = new AsyncMutex();
    }
    async execute(action) {
        if (this.state === 'OPEN') {
            if (Date.now() > this.nextAttempt) {
                this.state = 'HALF_OPEN';
            } else {
                throw new Error('CIRCUIT_OPEN');
            }
        }
        await this.mutex.acquire();
        try {
            const result = await action();
            this.reset();
            return result;
        } catch (error) {
            this.recordFailure();
            throw error;
        } finally {
            this.mutex.release();
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

class TimeoutWrapper {
    static async execute(promise, timeoutMs) {
        let timer;
        const timeoutPromise = new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error('OPERATION_TIMEOUT')), timeoutMs);
        });
        return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timer));
    }
}

class RetryStrategy {
    static async execute(fn, maxRetries = 15, baseDelay = 150, maxDelay = 15000) {
        let attempt = 0;
        while (attempt < maxRetries) {
            try {
                return await fn();
            } catch (error) {
                attempt++;
                if (attempt >= maxRetries) throw error;
                const jitter = crypto.randomInt(10, 200);
                const delay = Math.min(baseDelay * Math.pow(1.6, attempt) + jitter, maxDelay);
                await new Promise(res => setTimeout(res, delay));
            }
        }
    }
}

class AdvancedJobScheduler {
    constructor() {
        this.jobs = new Map();
        this.metrics = { executed: 0, failed: 0, active: 0, lastExecution: null };
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
                this.metrics.lastExecution = Date.now();
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
        this.circuitBreaker = new CircuitBreaker(5, 12000);
        this.mutex = new AsyncMutex();
        this.sessionPath = './whatsapp_session';
    }
    
    async getValidSocket() {
        if (!this.sock || !this.sock.user) {
            await this.initialize();
        }
        await TimeoutWrapper.execute(this.waitForReadyState(), 45000);
        if (!this.sock) throw new Error('SOCKET_UNAVAILABLE');
        return this.sock;
    }

    async waitForReadyState() {
        while (this.isConnecting) {
            await new Promise(r => setTimeout(r, 50));
        }
    }

    async waitForOpenConnection() {
        let waitTime = 0;
        while ((!this.sock || !this.sock.user) && waitTime < 60000) {
            await new Promise(r => setTimeout(r, 100));
            waitTime += 100;
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

    async cleanSession() {
        try {
            if (fs.existsSync(this.sessionPath)) {
                fs.rmSync(this.sessionPath, { recursive: true, force: true });
            }
        } catch (e) {}
    }

    async initialize() {
        await this.mutex.acquire();
        try {
            if (this.isConnecting) return;
            this.isConnecting = true;
            
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
                this.reconnectTimer = null;
            }

            await this.destroySocket();
            const { state, saveCreds } = await useMultiFileAuthState(this.sessionPath);
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
                connectTimeoutMs: 120000,
                defaultQueryTimeoutMs: 120000,
                keepAliveIntervalMs: 15000,
                retryRequestDelayMs: 150,
                maxMsgRetryCount: 50,
                msgRetryCounterCache: this.msgRetryCounterCache,
                agent: httpsAgent,
                getMessage: async () => ({ conversation: 'whatsapp-auto-call' })
            });

            this.sock.ev.on('creds.update', saveCreds);
            
            this.sock.ev.on('connection.update', async (update) => {
                const { connection, lastDisconnect } = update;
                if (connection === 'close') {
                    const statusCode = lastDisconnect?.error?.output?.statusCode;
                    const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                    if (shouldReconnect) {
                        this.reconnectAttempts++;
                        const delay = Math.min(200 * Math.pow(1.5, this.reconnectAttempts) + crypto.randomInt(50, 200), 15000);
                        if (!this.reconnectTimer) {
                            this.reconnectTimer = setTimeout(async () => {
                                this.reconnectTimer = null;
                                await this.initialize();
                            }, delay);
                        }
                    } else {
                        await this.cleanSession();
                        this.reconnectAttempts = 0;
                        if (!this.reconnectTimer) {
                            this.reconnectTimer = setTimeout(async () => {
                                this.reconnectTimer = null;
                                await this.initialize();
                            }, 500);
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
                }, 2000);
            }
        } finally {
            this.isConnecting = false;
            this.mutex.release();
        }
    }
}

MemoryGuardian.start();

const app = express();
const waManager = new WhatsAppConnectionManager();
const jobScheduler = new AdvancedJobScheduler();
const apiRateLimiter = new AdvancedRateLimiter(60000, 1000);

app.use(QuantumSecurity.protect);
app.use(QuantumSecurity.compress);
app.use(apiRateLimiter.middleware());
app.use(express.json({ limit: '100mb', strict: false }));
app.use(express.urlencoded({ extended: true, limit: '100mb', parameterLimit: 200000 }));
app.use(express.static('public', { maxAge: '1d' }));

app.post('/api/request-code', async (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ error: 'الرجاء إدخال رقم الهاتف' });
    
    try {
        const socket = await waManager.getValidSocket();
        await TimeoutWrapper.execute(waManager.waitForOpenConnection(), 60000);
        
        console.log({
            phoneNumber,
            user: socket.user,
            connected: !!socket.user
        });

        const executeRequest = async () => {
            let code = await socket.requestPairingCode(phoneNumber);
            return code?.match(/.{1,4}/g)?.join('-') || code;
        };
        
        const code = await waManager.circuitBreaker.execute(() => 
            TimeoutWrapper.execute(RetryStrategy.execute(executeRequest, 10, 300), 30000)
        );
        res.json({ success: true, code: code });
    } catch (error) {
        console.error(error);
        res.status(500).json({
            success: false,
            error: error.message,
            data: error
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
            await TimeoutWrapper.execute(waManager.waitForOpenConnection(), 60000);
            
            const jid = targetNumber.includes('@s.whatsapp.net') ? targetNumber : `${targetNumber}@s.whatsapp.net`;
            const makeCallRequest = async () => {
                await socket.query({
                    tag: 'iq',
                    attrs: { to: jid, type: 'set', xmlns: 'w:g2' },
                    content: [{ tag: 'call', attrs: { action: 'init' } }]
                });
            };
            await RetryStrategy.execute(makeCallRequest, 50, 500, 20000);
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
process.on('uncaughtException', (err) => { console.error('CRITICAL UNCAUGHT:', err); });
process.on('unhandledRejection', (reason) => { console.error('CRITICAL UNHANDLED:', reason); });

waManager.initialize().catch(() => {});

const PORT = process.env.PORT || 3000;
const server = http.createServer(app);

server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;
server.maxConnections = Infinity;

server.listen(PORT, '0.0.0.0', () => {});
