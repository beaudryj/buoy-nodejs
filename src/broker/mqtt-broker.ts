import type Logger from 'bunyan';
import { connect, IClientOptions, IClientPublishOptions, MqttClient } from 'mqtt';
import { Broker, DeliveryState, SendContext, SendOptions, Updater } from './broker';
import { CancelError, DeliveryError } from './errors';
import { Hash, Hasher } from '../hasher';
import fs from 'fs';
import path from 'path';

interface MqttBrokerOptions {
    mqtt_url: string;
    mqtt_cert?: string;
    mqtt_key?: string;
    mqtt_ca?: string;
    mqtt_expiry?: number;
}

interface Waiter {
    channel: string;
    hash: Hash;
    cb: (error?: Error, hash?: Hash) => void;
}

/** Passes messages and delivery notifications over MQTT. */
export class MqttBroker implements Broker {
    private client: MqttClient;
    private subscribers: Array<{ channel: string; updater: Updater }> = [];
    private waiting: Array<Waiter> = [];
    private expiry: number;
    private ended: boolean;
    private hasher = new Hasher();

    constructor(private options: MqttBrokerOptions, private logger: Logger) {
        this.ended = false;
        this.expiry = options.mqtt_expiry || 60 * 30;

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

        this.logger.info("Key Path:", keyPath);
        this.logger.info("Cert Path:", certPath);
        this.logger.info("CA Path:", caPath);

        const mqttOptions: IClientOptions = {
            clientId: `mqtt-client-${Math.random().toString(16).substr(2, 8)}`,
            rejectUnauthorized: true,
            key: fs.readFileSync(keyPath),
            cert: fs.readFileSync(certPath),
            ca: fs.readFileSync(caPath),
            reconnectPeriod: 10000, // ⏳ Increase reconnection period to 10 seconds
            keepalive: 60, // ⏳ Keepalive interval to prevent frequent disconnects
        };

        this.client = connect(options.mqtt_url, mqttOptions);
        this.client.on('message', this.messageHandler.bind(this));
        this.client.on('close', () => {
            if (!this.ended) {
                this.logger.warn('⚠️ Disconnected from MQTT broker');
            }
        });
        this.client.on('connect', () => {
            this.logger.info('✅ Connected to MQTT broker');
        });
        this.client.on('reconnect', () => {
            this.logger.info('🔄 Attempting MQTT reconnect');
        });
        this.client.on('error', (error) => {
            this.logger.error('❌ MQTT Error:', error);
        });
    }

    async init() {
        this.logger.info('📡 Initializing MQTT broker %s', this.options.mqtt_url);
        if (!this.client.connected) {
            await new Promise<void>((resolve, reject) => {
                this.client.once('connect', () => {
                    this.client.removeListener('error', reject);
                    resolve();
                });
                this.client.once('error', reject);
            });
        }
    }

    async deinit() {
        this.logger.debug('📴 MQTT broker deinit');
        this.ended = true;
        const cancels: Promise<void>[] = [];
        for (const { hash, channel, cb } of this.waiting) {
            this.logger.info({ hash, channel }, 'Cancelling waiter');
            cancels.push(this.sendCancel(channel, hash));
            cb(new CancelError('Shutting down'));
        }
        this.waiting = [];
        await Promise.all(cancels);
        await new Promise<void>((resolve, reject) => {
            this.client.end(false, {}, (error) => {
                if (error) {
                    reject(error);
                } else {
                    resolve();
                }
            });
        });
    }

    async healthCheck() {
        if (!this.client.connected) {
            throw new Error('Lost connection to MQTT broker');
        }
    }

    async send(channel: string, payload: Buffer, options: SendOptions, ctx?: SendContext) {
        const hash = this.hasher.hash(payload);
        this.logger.debug({ hash, channel }, 'Sending message %s', hash);
        const cancel = () => {
            this.sendCancel(channel, hash).catch((error) => {
                this.logger.warn(error, 'Error during send cancel');
            });
        };
        if (ctx) {
            ctx.cancel = cancel;
        }
        const timeout = (options.wait || 0) * 1000;
        let deliveryPromise: Promise<void> | undefined;
        if (timeout > 0) {
            deliveryPromise = this.waitForDelivery(channel, timeout, hash);
            deliveryPromise.catch(() => {}); // Prevent unhandled rejection
        }
        await this.publish(`channel/${channel}`, payload, {
            qos: 2,
            retain: true,
            properties: {
                messageExpiryInterval: this.expiry,
            },
        });
        let rv = DeliveryState.buffered;
        if (deliveryPromise) {
            try {
                await deliveryPromise;
                rv = DeliveryState.delivered;
            } catch (error) {
                if (ctx) {
                    ctx.cancel = undefined;
                }
                if (error instanceof DeliveryError && options.requireDelivery) {
                    cancel();
                    throw error;
                }
            }
        }
        if (ctx) {
            ctx.cancel = undefined;
        }
        return rv;
    }

    private async sendCancel(channel: string, hash?: Hash) {
        await this.publish(`channel/${channel}`, '', { qos: 1, retain: true });
        await this.publish(`cancel/${channel}`, hash?.bytes || '', { qos: 1 });
    }

    private messageHandler(topic: string, payload: Buffer) {
        const parts = topic.split('/');
        switch (parts[0]) {
            case 'delivery':
                this.handleDelivery(parts[1], payload.byteLength > 0 ? new Hash(payload) : undefined);
                break;
            case 'channel':
                this.handleChannelMessage(parts[1], payload);
                break;
            case 'cancel':
                this.handleCancel(parts[1], payload.byteLength > 0 ? new Hash(payload) : undefined);
                break;
            default:
                this.logger.warn({ topic }, 'Unexpected MQTT message');
        }
    }

    private handleChannelMessage(channel: string, payload: Buffer) {
        if (payload.length === 0) return;
        const hash = this.hasher.hash(payload);
        const updaters = this.subscribers.filter((sub) => sub.channel === channel).map((sub) => sub.updater);
        this.logger.debug({ channel, hash }, 'Updating %d subscription(s)', updaters.length);
        Promise.allSettled(updaters.map((fn) => fn(payload))).then(() => {
            this.client.publish(`channel/${channel}`, '', { qos: 1, retain: true });
            this.client.publish(`delivery/${channel}`, hash.bytes, { qos: 1 });
        });
    }

    private handleDelivery(channel: string, hash?: Hash) {
        this.logger.debug({ channel, hash }, 'Message delivered');
        this.waiting.filter((item) => item.channel === channel).forEach((waiter) => waiter.cb(undefined, hash));
    }

    private async publish(topic: string, payload: string | Buffer, options: IClientPublishOptions = {}) {
        return new Promise<void>((resolve, reject) => {
            this.client.publish(topic, payload, options, (error) => (error ? reject(error) : resolve()));
        });
    }
}
