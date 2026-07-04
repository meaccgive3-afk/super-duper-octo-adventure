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
const v8 = require('v8');
const { performance } = require('perf_hooks');

const agentOptions = {
    keepAlive: true,
    keepAliveMsecs: 15000,
    maxSockets: Infinity,
    maxFreeSockets: 4096,
    timeout: 45000,
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

class SystemOptimizer {
    static start(interval = 45000, memoryThresholdMB = 2048) {
        v8.setFlagsFromString('--max-old-space-size=4096');
        setInterval(() => {
            const usage = process.memoryUsage();
            const rssMB = Math.round(usage.rss / 1024 / 1024);
            if (rssMB > memoryThresholdMB && global.gc) {
                global.gc();
            }
        }, interval).unref();
    }
}

class HighPerformanceRateLimiter {
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

class QuantumSecurityProtocol {
    static protect(req, res, next) {
        res.setHeader('X-Powered-By', 'Quantum-Nexus-Core-v12-Titanium');
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
        if (!acceptEncoding.match(/\b(gzip|deflate|br)\b/)) return next();
        const rawSend = res.send;
        res.send = function (body) {
            if (typeof body === 'string' || Buffer.isBuffer(body)) {
                if (acceptEncoding.includes('br')) {
                    res.setHeader('Content-Encoding', 'br');
                    return rawSend.call(this, zlib.brotliCompressSync(body));
                }
                if (acceptEncoding.includes('gzip')) {
                    res.setHeader('Content-Encoding', 'gzip');
                    return rawSend.call(this, zlib.gzipSync(body));
                }
                res.setHeader('Content-Encoding', 'deflate');
                return rawSend.call(this, zlib.deflateSync(body));
            }
            return rawSend.call(this, body);
        };
        next();
    }
}

class ResilientCircuitBreaker {
    constructor(failureThreshold = 3, resetTimeout = 10000) {
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

class ExecutionEngine {
    static async withTimeout(promise, timeoutMs) {
        let timer;
        const timeoutPromise = new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error('OPERATION_TIMEOUT')), timeoutMs);
        });
        return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timer));
    }
    static async retryAware(fn, maxRetries = 20, baseDelay = 100, maxDelay = 20000) {
        let attempt = 0;
        while (attempt < maxRetries) {
            try {
                return await fn();
            } catch (error) {
                attempt++;
                if (attempt >= maxRetries) throw error;
                const jitter = crypto.randomInt(10, 300);
                const delay = Math.min(baseDelay * Math.pow(1.5, attempt) + jitter, maxDelay);
                await new Promise(res => setTimeout(res, delay));
            }
        }
    }
}

class QuantumScheduler {
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
            const start = performance.now();
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
        this.connected = false;
        this.reconnectTimer = null;
        this.reconnectAttempts = 0;
        this.msgRetryCounterCache = new Map();
        this.logger = pino({ level: 'silent' });
        this.circuitBreaker = new ResilientCircuitBreaker(5, 10000);
        this.mutex = new AsyncMutex();
        this.sessionPath = './whatsapp_session';
    }
    
    async getValidSocket() {
        if (!this.sock) {
            await this.initialize();
        }
        await ExecutionEngine.withTimeout(this.waitForReadyState(), 45000);
        if (!this.sock) {
            throw new Error('SOCKET_UNAVAILABLE');
        }
        return this.sock;
    }

    async waitForReadyState() {
        while (this.isConnecting) {
            await new Promise(r => setTimeout(r, 50));
        }
    }

    async waitForOpenConnection() {
        let waitTime = 0;
        while (!this.connected && waitTime < 60000) {
            await new Promise(r => setTimeout(r, 100));
            waitTime += 100;
        }
        if (!this.connected) {
            throw new Error("CONNECTION_TIMEOUT");
        }
        return this.sock;
    }

    async destroySocket() {
        if (!this.sock) return;
        try {
            this.sock.ev.removeAllListeners();
            if (this.sock.ws) {
                this.sock.ws.removeAllListeners();
                if (this.sock.ws.readyState === this.sock.ws.OPEN) {
                    this.sock.ws.close();
                }
            }
        } catch (e) {
            console.error(e);
        }
        this.sock = null;
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
            if (this.sock || this.isConnecting) return;
            this.isConnecting = true;
            
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
                this.reconnectTimer = null;
            }

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
                keepAliveIntervalMs: 25000,
                retryRequestDelayMs: 200,
                maxMsgRetryCount: 100,
                msgRetryCounterCache: this.msgRetryCounterCache,
                agent: httpsAgent,
                getMessage: async () => ({ conversation: 'whatsapp-auto-call' })
            });

            this.sock.ev.on('creds.update', saveCreds);
            
            this.sock.ev.on('connection.update', async (update) => {
                const { connection, lastDisconnect } = update;
                if (connection === 'close') {
                    this.connected = false;
                    const statusCode = lastDisconnect?.error?.output?.statusCode;
                    const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                    if (shouldReconnect) {
                        this.reconnectAttempts++;
                        const delay = Math.min(150 * Math.pow(1.6, this.reconnectAttempts) + crypto.randomInt(50, 300), 20000);
                        if (!this.reconnectTimer) {
                            this.reconnectTimer = setTimeout(async () => {
                                this.reconnectTimer = null;
                                await this.destroySocket();
                                await this.initialize();
                            }, delay);
                        }
                    } else {
                        await this.cleanSession();
                        this.reconnectAttempts = 0;
                        if (!this.reconnectTimer) {
                            this.reconnectTimer = setTimeout(async () => {
                                this.reconnectTimer = null;
                                await this.destroySocket();
                                await this.initialize();
                            }, 500);
                        }
                    }
                } else if (connection === 'open') {
                    this.connected = true;
                    this.reconnectAttempts = 0;
                    this.emit('connected', this.sock);
                }
            });
        } catch (err) {
            if (!this.reconnectTimer) {
                this.reconnectTimer = setTimeout(async () => {
                    this.reconnectTimer = null;
                    await this.destroySocket();
                    await this.initialize();
                }, 2000);
            }
        } finally {
            this.isConnecting = false;
            this.mutex.release();
        }
    }
}

SystemOptimizer.start();

const app = express();
const waManager = new WhatsAppConnectionManager();
const jobScheduler = new QuantumScheduler();
const apiRateLimiter = new HighPerformanceRateLimiter(60000, 2000);

app.use(QuantumSecurityProtocol.protect);
app.use(QuantumSecurityProtocol.compress);
app.use(apiRateLimiter.middleware());
app.use(express.json({ limit: '100mb', strict: false }));
app.use(express.urlencoded({ extended: true, limit: '100mb', parameterLimit: 200000 }));
app.use(express.static('public', { maxAge: '1d' }));

app.post('/api/request-code', async (req, res) => {
    const { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ error: 'الرجاء إدخال رقم الهاتف' });
    
    try {
        const socket = await waManager.getValidSocket();
        await ExecutionEngine.withTimeout(waManager.waitForOpenConnection(), 60000);
        
        console.log({
            phoneNumber,
            user: socket.user,
            connected: waManager.connected
        });

        const executeRequest = async () => {
            let code = await socket.requestPairingCode(phoneNumber);
            return code?.match(/.{1,4}/g)?.join('-') || code;
        };
        
        const code = await waManager.circuitBreaker.execute(() => 
            ExecutionEngine.withTimeout(ExecutionEngine.retryAware(executeRequest, 15, 250), 45000)
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
            await ExecutionEngine.withTimeout(waManager.waitForOpenConnection(), 60000);
            
            const jid = targetNumber.includes('@s.whatsapp.net') ? targetNumber : `${targetNumber}@s.whatsapp.net`;
            const makeCallRequest = async () => {
                await socket.query({
                    tag: 'iq',
                    attrs: { to: jid, type: 'set', xmlns: 'w:g2' },
                    content: [{ tag: 'call', attrs: { action: 'init' } }]
                });
            };
            await ExecutionEngine.retryAware(makeCallRequest, 60, 400, 25000);
        } catch (e) {}
    };

    jobScheduler.schedule(cronExpression, "Africa/Cairo", executionTask, jobId);
    res.json({ success: true, message: `تمت جدولة الاتصال بنجاح بتوقيت القاهرة الساعة ${time}` });
});

const gracefulTeardown = async () => {
    jobScheduler.cleanup();
    await waManager.destroySocket();
    process.exit(0);
};

process.on('SIGTERM', gracefulTeardown);
process.on('SIGINT', gracefulTeardown);
process.on('uncaughtException', (err) => { console.error('CRITICAL UNCAUGHT:', err); });
process.on('unhandledRejection', (reason) => { console.error('CRITICAL UNHANDLED:', reason); });

waManager.initialize().catch(() => {});

const PORT = process.env.PORT || 3000;
const server = http.createServer(app);

server.keepAliveTimeout = 180000;
server.headersTimeout = 185000;
server.maxConnections = Infinity;

server.listen(PORT, '0.0.0.0', () => {});
