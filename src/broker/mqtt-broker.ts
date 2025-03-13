import type Logger from 'bunyan'
import { connect, IClientOptions, IClientPublishOptions, MqttClient } from 'mqtt'
import { Broker, DeliveryState, SendContext, SendOptions, Updater } from './broker'
import { CancelError, DeliveryError } from './errors'
import { Hash, Hasher } from '../hasher'
import fs from 'fs'
import path from 'path'

interface MqttBrokerOptions {
    mqtt_url: string
    mqtt_cert?: string
    mqtt_key?: string
    mqtt_ca?: string
    mqtt_expiry?: number
}

interface Waiter {
    channel: string
    hash: Hash
    cb: (error?: Error, hash?: Hash) => void
}

/** Passes messages and delivery notifications over MQTT. */
export class MqttBroker implements Broker {
    private client: MqttClient
    private subscribers: Array<{ channel: string; updater: Updater }> = []
    private waiting: Array<Waiter> = []
    private expiry: number
    private ended: boolean
    private hasher = new Hasher()

    constructor(private options: MqttBrokerOptions, private logger: Logger) {
        this.ended = false
        this.expiry = options.mqtt_expiry || 60 * 30

        const CERTS_DIR = "/tmp/certs";
        if (!fs.existsSync(CERTS_DIR)) {
            fs.mkdirSync(CERTS_DIR, { recursive: true });
        }

        const keyPath = path.join(CERTS_DIR, "private.key");
        const certPath = path.join(CERTS_DIR, "certificate.pem");
        const caPath = path.join(CERTS_DIR, "AmazonRootCA1.pem");

        fs.writeFileSync(keyPath, options.mqtt_key || '');
        fs.writeFileSync(certPath, options.mqtt_cert || '');
        fs.writeFileSync(caPath, options.mqtt_ca || '');

        this.logger.info("🔍 MQTT Broker Certificates Loaded");

        const mqttOptions: IClientOptions = {
            clientId: `mqtt-broker-${Math.random().toString(16).substr(2, 8)}`,
            rejectUnauthorized: true,
            key: fs.readFileSync(keyPath),
            cert: fs.readFileSync(certPath),
            ca: fs.readFileSync(caPath),
            keepalive: 90,        // 🔄 Extend keepalive to 90s to prevent idle disconnects
            reconnectPeriod: 15000, // 🔄 Increase to 15s to avoid AWS throttling reconnect attempts
            clean: true           // Ensures the session resets properly if needed
        }

        this.client = connect(options.mqtt_url, mqttOptions)
        this.client.on('message', this.messageHandler.bind(this))
        
        this.client.on('close', () => {
            if (!this.ended) {
                this.logger.warn('⚠️ MQTT Client Disconnected from Server');
            }
        })

        this.client.on('connect', () => {
            this.logger.info('✅ Connected to AWS IoT MQTT!');
            this.delaySubscription();
        })

        this.client.on('reconnect', () => {
            this.logger.info('🔄 Attempting MQTT Reconnection...');
        })
    }

    private delaySubscription() {
        setTimeout(() => {
            this.logger.info("📥 Attempting Subscription...");
            for (const sub of this.subscribers) {
                this.client.subscribe(`channel/${sub.channel}`, { qos: 1 }, (err, granted) => {
                    if (err) {
                        this.logger.error("❌ Subscription Error:", err);
                    } else {
                        this.logger.info("✅ Successfully Subscribed:", granted);
                    }
                });
            }
        }, 3000);  // 🔄 Delay subscription by 3s to ensure AWS IoT allows it
    }

    async init() {
        this.logger.info('🚀 Initializing MQTT Broker: %s', this.options.mqtt_url)
        if (!this.client.connected) {
            await new Promise<void>((resolve, reject) => {
                this.client.once('connect', () => {
                    this.client.removeListener('error', reject)
                    resolve()
                })
                this.client.once('error', reject)
            })
        }
    }

    async healthCheck() {
        if (!this.client.connected) {
            throw new Error('⚠️ Lost Connection to MQTT Server')
        }
    }

    async send(channel: string, payload: Buffer, options: SendOptions, ctx?: SendContext) {
        const hash = this.hasher.hash(payload)
        this.logger.debug({ hash, channel }, '📤 Sending Message to %s', channel)

        const cancel = () => {
            this.sendCancel(channel, hash).catch((error) => {
                this.logger.warn(error, '⚠️ Error During Send Cancel')
            })
        }

        if (ctx) ctx.cancel = cancel
        const timeout = (options.wait || 0) * 1000

        let deliveryPromise: Promise<void> | undefined
        if (timeout > 0) {
            deliveryPromise = this.waitForDelivery(channel, timeout, hash)
            deliveryPromise.catch(() => {})
        }

        await this.publish(`channel/${channel}`, payload, {
            qos: 2,
            retain: true,
            properties: { messageExpiryInterval: this.expiry },
        })

        let rv = DeliveryState.buffered
        if (deliveryPromise) {
            try {
                await deliveryPromise
                rv = DeliveryState.delivered
            } catch (error) {
                if (ctx) ctx.cancel = undefined
                if (error instanceof DeliveryError && options.requireDelivery) {
                    cancel()
                    throw error
                } else {
                    throw error
                }
            }
        }
        if (ctx) ctx.cancel = undefined
        return rv
    }

    async subscribe(channel: string, updater: Updater) {
        this.logger.debug({ channel }, '📥 New Subscription Requested')
        const sub = { channel, updater }
        await this.addSubscriber(sub)
        return () => {
            this.logger.debug({ channel }, '📤 Unsubscribing...')
            this.removeSubscriber(sub)
        }
    }

    private async sendCancel(channel: string, hash?: Hash) {
        await this.publish(`channel/${channel}`, '', { qos: 1, retain: true })
        await this.publish(`cancel/${channel}`, hash?.bytes || '', { qos: 1 })
    }

    private messageHandler(topic: string, payload: Buffer) {
        const parts = topic.split('/')
        switch (parts[0]) {
            case 'delivery':
                this.handleDelivery(parts[1], payload.byteLength > 0 ? new Hash(payload) : undefined)
                break
            case 'channel':
                this.handleChannelMessage(parts[1], payload)
                break
            case 'cancel':
                this.handleCancel(parts[1], payload.byteLength > 0 ? new Hash(payload) : undefined)
                break
            default:
                this.logger.warn({ topic }, '⚠️ Unexpected MQTT Message')
        }
    }

    private async publish(topic: string, payload: string | Buffer, options: IClientPublishOptions = {}) {
        return new Promise<void>((resolve, reject) => {
            this.client.publish(topic, payload, options, (error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
    }
}
