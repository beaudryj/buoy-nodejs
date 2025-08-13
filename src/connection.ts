import WebSocket from 'ws'

import logger from './logger'

const HEARTBEAT_INTERVAL = 10 * 1000

/** A WebSocket connection. */
export class Connection {
    static seq = 0

    alive: boolean
    closed: boolean
    id: number
    version: number

    private cleanupCallback: () => void
    private socket: WebSocket
    private timer: NodeJS.Timeout
    private log: typeof logger

    /**
     * Simple ACK protocol, like STOMP but simpler and using binary frames instead of text.
     *
     * Format: 0x4242<type>[payload]
     *
     * For version 2 clients the server will add a 0x4242<type> header to all messages sent.
     *
     * When a message is delivered with 0x424201<seq><payload> the client send back a
     * 0x424202<seq> to acknowledge receiving the message.
     */
    private ackSeq = 0
    private ackWaiting: { [seq: number]: () => void } = {}

    constructor(socket: WebSocket, version: number, cleanup: () => void) {
        this.id = ++Connection.seq
        this.log = logger.child({ conn: this.id })
        this.socket = socket
        this.closed = false
        this.alive = true
        this.cleanupCallback = cleanup
        this.version = version
        this.socket.on('close', () => {
            this.didClose()
        })
        this.socket.on('message', (data: any, isBinary: boolean) => {
            if (!isBinary) data = Buffer.from(data, 'utf8')
            this.handleMessage(data)
        })
        this.socket.on('pong', () => {
            this.alive = true
        })
        this.socket.on('error', (error) => {
            this.log.warn(error, 'socket error')
        })
        this.timer = setInterval(() => {
            if (this.alive) {
                this.alive = false
                this.socket.ping()
            } else {
                this.destroy()
            }
        }, HEARTBEAT_INTERVAL)
    }

    private didClose() {
        this.log.debug({ alive: this.alive, closed: this.closed }, 'did close')
        this.alive = false
        clearTimeout(this.timer)
        if (this.closed === false) {
            this.cleanupCallback()
        }
        this.closed = true
    }

    async send(data: Buffer) {
        if (this.closed) {
            throw new Error('Socket closed')
        }
        if (this.version === 2) {
            await this.ackSend(data)
        } else {
            this.log.debug({ size: data.byteLength }, 'send data')
            this.socket.send(data)
        }
    }

    close(code?: number, reason?: string) {
        this.socket.close(code, reason)
        this.didClose()
    }

    destroy() {
        this.socket.terminate()
        this.didClose()
    }

    private handleMessage(data: Buffer) {
        if (data[0] !== 0x42 || data[1] !== 0x42) return
        const type = data[2]
        this.log.debug({ type }, 'command message')
        switch (type) {
            case 0x02: {
                const seq = data[3]
                const callback = this.ackWaiting[seq]
                if (callback) {
                    callback()
                }
                break
            }
        }
    }

    private async ackSend(data: Buffer) {
        const seq = ++this.ackSeq % 255
        this.log.debug({ size: data.byteLength, seq }, 'ack send data')
        const header = Buffer.from([0x42, 0x42, 0x01, seq])
        data = Buffer.concat([header, data])
        this.socket.send(data)
        return await this.waitForAck(seq)
    }

    private waitForAck(seq: number, timeout = 5000) {
        return new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => {
                delete this.ackWaiting[seq]
                reject(new Error('Timed out waiting for ACK'))
                this.destroy()
            }, timeout)
            this.ackWaiting[seq] = () => {
                this.log.debug({ seq }, 'got ack')
                clearTimeout(timer)
                delete this.ackWaiting[seq]
                resolve()
            }
        })
    }
}
