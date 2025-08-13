import { URL } from 'url'
import * as http from 'http'
import * as https from 'https'
import * as os from 'os'
import WebSocket from 'ws'
import cluster, { Worker } from 'cluster'
import config from 'config'
import type Logger from 'bunyan'

import logger from './logger'
import setupBroker, {
    Broker,
    CancelError,
    DeliveryError,
    DeliveryState,
    SendContext,
    Unsubscriber,
    Updater,
} from './broker'
import version from './version'
import { getUUID, HttpError, readBody } from './utils'
import { Status } from './interfaces'
import { Connection } from './connection'
import { Hash } from './hasher'

let broker: Broker
let requestSeq = 0
let activeRequests = 0

let actAsProxy = false

const connections: Connection[] = []

const BUOY_SERVICE = 'cb.anchor.link'

type UpdaterWithType = (payload: Buffer, type: string) => Promise<void>

async function handleWSConnection(socket: WebSocket, request: http.IncomingMessage) {
    const uuid = getUUID(request)
    let version = 1
    if (request.url) {
        const query = new URL(request.url, 'http://localhost').searchParams
        version = Number.parseInt(query.get('v') || '') || 1
    }
    let unsubscribe: Unsubscriber | null = null
    let unsubscribeBuoy: Unsubscriber | null = null
    let prematureClose = false
    const connection = new Connection(socket, version, () => {
        log.debug('connection closed')
        let unsubscribeDone = 0
        if (unsubscribe) {
            unsubscribeDone++
            unsubscribe()
        }
        if (unsubscribeBuoy) {
            unsubscribeDone++
            unsubscribeBuoy()
        }

        if (unsubscribeDone !== 2) {
            prematureClose = true
        }

        connections.splice(connections.indexOf(connection), 1)
    })
    connections.push(connection)
    const log = logger.child({ uuid, conn: connection.id })

    log.debug('new connection')

    const updater: UpdaterWithType = createUpdater(connection, log)

    unsubscribe = await broker.subscribe(uuid, async (data) => {
        updater(data, 'broker')
    })

    if (actAsProxy) {
        unsubscribeBuoy = await listenToBuoy(uuid, log, async (data) => {
            updater(data, 'buoy')
        })
    }

    if (prematureClose) {
        if (unsubscribe) {
            unsubscribe()
        }
        if (unsubscribeBuoy) {
            unsubscribeBuoy()
        }
    }
}

function createUpdater(connection: Connection, log: Logger): UpdaterWithType {
    let hashSet: { hash: Hash; delivered: number }[] = []

    const cleanHashSet = (delivered: number) => {
        hashSet = hashSet.filter((_) => delivered - _.delivered <= (2 * 60 + 10) * 1000)
    }

    return async (data, type) => {
        log.debug('delivering payload using', type)
        const hash = data.byteLength > 0 ? new Hash(data) : undefined
        if (hash) {
            cleanHashSet(Date.now())

            if (!hashSet.some((_) => _.hash.equals(hash))) {
                hashSet.push({ hash, delivered: Date.now() })
                try {
                    await connection.send(data)
                } catch (error) {
                    log.info(error, 'failed to deliver payload')
                    throw error
                }
                log.info('payload delivered')
            } else {
                log.info('payload was already delivered')
            }
        }
    }
}

async function listenToBuoy(uuid: string, log: Logger, updater: Updater): Promise<Unsubscriber> {
    log.debug(
        { url: `wss://${BUOY_SERVICE}/${uuid}`, channel: uuid },
        'start listening proxy websocket'
    )

    const ws = new WebSocket(`wss://${BUOY_SERVICE}/${uuid}`)

    ws.on('open', () => {
        log.debug({ channel: uuid }, 'successfully opened proxy websocket connection')
    })

    ws.on('error', (e) => {
        log.error(e, 'problem with proxy websocket connection')
    })

    ws.on('message', async (data: any, isBinary: boolean) => {
        if (!isBinary) data = Buffer.from(data, 'utf8')
        updater(data)
    })

    ws.on('close', () => {
        log.debug({ channel: uuid }, 'successfully unsubscribed from proxy websocket connection')
    })

    return () => {
        log.debug({ channel: uuid }, 'unsubscribe from proxy websocket connection')
        ws.close()
    }
}

async function handlePost(
    request: http.IncomingMessage,
    response: http.ServerResponse,
    log: Logger
) {
    const uuid = getUUID(request)
    log = log.child({ uuid })
    const data = await readBody(request)
    if (data.byteLength === 0) {
        throw new HttpError('Unable to forward empty message', 400)
    }
    const ctx: SendContext = {}
    request.once('close', () => {
        response.end()
        if (ctx.cancel) {
            ctx.cancel()
        }
    })

    const waitHeader = request.headers['x-buoy-wait'] || request.headers['x-buoy-soft-wait']
    const requireDelivery = !!request.headers['x-buoy-wait']
    let wait = 0
    if (waitHeader) {
        wait = Math.min(Number(waitHeader), 120)
        if (!Number.isFinite(wait)) {
            throw new HttpError('Invalid wait timeout', 400)
        }
    }

    const sendToBroker = async () => await broker.send(uuid, data, { wait, requireDelivery }, ctx)

    const promisesToTrack: Promise<DeliveryState>[] = [sendToBroker()]

    if (actAsProxy) {
        promisesToTrack.push(forwardPostRequest(uuid, data, request, log))
    }

    try {
        const delivery = await Promise.race(promisesToTrack)
        response.setHeader('X-Buoy-Delivery', delivery)
        log.info({ delivery }, 'message dispatched')
        if (wait > 0 && delivery == DeliveryState.buffered) {
            return 202
        }
    } catch (error) {
        if (error instanceof CancelError) {
            throw new HttpError(`Request cancelled (${error.reason})`, 410)
        } else if (error instanceof DeliveryError) {
            throw new HttpError(`Unable to deliver message (${error.reason})`, 408)
        }
    }
}

function forwardPostRequest<T>(
    uuid: string,
    body: T,
    request: http.IncomingMessage,
    log: Logger,
    ctx?: SendContext
): Promise<DeliveryState> {
    return new Promise((resolve, reject) => {
        const headers: https.RequestOptions['headers'] = {}
        if (request.headers['x-buoy-wait']) {
            headers['x-buoy-wait'] = request.headers['x-buoy-wait']
        }

        if (request.headers['x-buoy-soft-wait']) {
            headers['x-buoy-soft-wait'] = request.headers['x-buoy-soft-wait']
        }

        const options: https.RequestOptions = {
            host: BUOY_SERVICE,
            port: 443,
            path: `/${uuid}`,
            method: 'POST',
            headers: headers,
        }

        const cbAnchorRequest = https.request(options, (res) => {
            res.setEncoding('utf8')

            res.on('data', (chunk) => {
                log.info({ body: chunk }, 'forwarded request data dispatched')
            })

            res.on('end', () => {
                const delivery = res.headers['X-Buoy-Delivery'] as DeliveryState
                log.info({ delivery }, 'forwarded request sent')

                resolve(delivery)
            })
        })

        if (ctx) {
            ctx.cancel = () => {
                log.warn('Cancel sending forwarded request')
                cbAnchorRequest.destroy(new HttpError('Request canceled', 410))
            }
        }

        cbAnchorRequest.on('error', (e) => {
            log.error(e, 'problem with forwarded request')
            reject(e)
        })
        cbAnchorRequest.write(body)
        cbAnchorRequest.end()
    })
}

function handleRequest(request: http.IncomingMessage, response: http.ServerResponse) {
    response.setHeader('Server', `buoy/${version}`)
    response.setHeader('Access-Control-Allow-Origin', '*')
    response.setHeader('Access-Control-Allow-Headers', '*')
    response.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS')
    response.setHeader('Access-Control-Expose-Headers', 'X-Buoy-Delivery')
    if (request.url === '/health_check') {
        broker
            .healthCheck()
            .then(() => {
                response.statusCode = 200
                response.write('Ok')
                response.end()
            })
            .catch((error) => {
                response.statusCode = 500
                response.write(error.message || String(error))
                response.end()
            })
        return
    }
    if (request.method !== 'POST') {
        response.setHeader('Allow', 'POST, OPTIONS')
        response.statusCode = request.method === 'OPTIONS' ? 200 : 405
        response.end()
        return
    }
    if (request.url === '/test') {
        response.statusCode = 200
        response.write('Ok')
        response.end()
        return
    }
    activeRequests++
    const log = logger.child({ req: ++requestSeq })
    handlePost(request, response, log)
        .then((status) => {
            response.statusCode = status || 200
            response.write('Ok')
            response.end()
        })
        .catch((error) => {
            if (response.writableEnded) {
                log.debug(error, 'error from ended request')
                return
            }
            if (error instanceof HttpError) {
                log.info('http %d when handling post request: %s', error.statusCode, error.message)
                response.statusCode = error.statusCode
                response.write(error.message)
            } else {
                log.error(error, 'unexpected error handling post request')
                response.statusCode = 500
                response.write('Internal server error')
            }
            response.end()
        })
        .finally(() => {
            activeRequests--
        })
}

async function setup(port: number) {
    const httpServer = http.createServer(handleRequest)
    const websocketServer = new WebSocket.Server({ server: httpServer })
    broker = await setupBroker()
    await new Promise<void>((resolve, reject) => {
        httpServer.listen(port, resolve)
        httpServer.once('error', reject)
    })

    websocketServer.on('connection', (socket, request) => {
        handleWSConnection(socket as any, request).catch((error) => {
            logger.error(error, 'error handling websocket connection')
            socket.close()
        })
    })

    return async () => {
        const close = new Promise<void>((resolve, reject) => {
            httpServer.close((error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
        connections.map((c) => c.close(1012, 'Shutting down'))
        await Promise.all([close, broker.deinit()])
    }
}

export async function main() {
    const port = Number.parseInt(config.get('port'))

    actAsProxy = config.has('act_as_proxy')

    logger.info(
        actAsProxy ? `acting as a proxy for ${BUOY_SERVICE}` : 'acting as a standalone service'
    )

    if (!Number.isFinite(port)) {
        throw new Error('Invalid port number')
    }
    if (cluster.isPrimary) {
        logger.info({ version }, 'starting')
    }
    let numWorkers = Number.parseInt(config.get('num_workers'), 10)
    if (numWorkers === 0) {
        numWorkers = os.cpus().length
    }
    const isPrimary = cluster.isPrimary && numWorkers > 1
    let teardown: () => Promise<void> | undefined
    let statusTimer: any
    const statusInterval = (Number(config.get('status_interval')) || 0) * 1000
    if (isPrimary) {
        const workers: Worker[] = []
        logger.info('spawning %d workers', numWorkers)
        const runningPromises: Promise<void>[] = []
        for (let i = 0; i < numWorkers; i++) {
            const worker = cluster.fork()
            const running = new Promise<void>((resolve, reject) => {
                worker.once('message', (message) => {
                    if (message.error) {
                        reject(new Error(message.error))
                    } else {
                        resolve()
                    }
                })
            })
            runningPromises.push(running)
            workers.push(worker)
        }
        await Promise.all(runningPromises)
        if (statusInterval > 0) {
            const workerStatus: Record<number, Status> = {}
            workers.forEach((worker) => {
                worker.on('message', (message) => {
                    if (message.status) {
                        workerStatus[worker.id] = message.status
                    }
                })
            })
            statusTimer = setInterval(() => {
                const status: Status = {
                    activeConnections: 0,
                    activeRequests: 0,
                    numConnections: 0,
                    numRequests: 0,
                }
                for (const s of Object.values(workerStatus)) {
                    status.activeConnections += s.activeConnections
                    status.activeRequests += s.activeRequests
                    status.numConnections += s.numConnections
                    status.numRequests += s.numRequests
                }
                logger.info(status, 'status')
            }, statusInterval)
        }
    } else {
        try {
            teardown = await setup(port)
            if (process.send) {
                process.send({ ready: true })
            }
        } catch (error) {
            if (process.send) {
                process.send({ error: (error as Error).message || String(error) })
            }
            throw error
        }
        if (statusInterval > 0) {
            statusTimer = setInterval(() => {
                const status: Status = {
                    activeConnections: connections.length,
                    activeRequests,
                    numConnections: Connection.seq,
                    numRequests: requestSeq,
                }
                if (process.send) {
                    process.send({ status })
                } else {
                    logger.info(status, 'status')
                }
            }, statusInterval)
        }
    }

    async function exit() {
        clearInterval(statusTimer)
        if (teardown) {
            const timeout = new Promise<never>((_, reject) => {
                setTimeout(() => {
                    reject(new Error('Timed out waiting for teardown'))
                }, 10 * 1000)
            })
            await Promise.race([teardown(), timeout])
        }
        return 0
    }

    process.on('SIGTERM', () => {
        if (cluster.isPrimary) {
            logger.info('got SIGTERM, exiting...')
        }
        exit()
            .then((code) => {
                logger.debug('bye')
                process.exit(code)
            })
            .catch((error) => {
                logger.fatal(error, 'unable to exit gracefully')
                setTimeout(() => process.exit(1), 1000)
            })
    })

    if (cluster.isPrimary) {
        logger.info({ port }, 'server running')
    }
}

if (module === require.main) {
    process.once('uncaughtException', (error) => {
        logger.error(error, 'Uncaught exception')
        if (cluster.isPrimary) {
            abort(1)
        }
    })
    main().catch((error) => {
        if (cluster.isPrimary) {
            logger.fatal(error, 'Unable to start application')
        }
        abort(1)
    })
}

function abort(code: number) {
    process.exitCode = code
    setImmediate(() => {
        process.exit(code)
    })
}
