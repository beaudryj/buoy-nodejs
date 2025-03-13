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

/** Passes messages and delivery notifications over AWS IoT MQTT. */
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

        this.logger.info("AWS IoT Key Path:", keyPath);
        this.logger.info("AWS IoT Cert Path:", certPath);
        this.logger.info("AWS IoT CA Path:", caPath);

        const mqttOptions: IClientOptions = {
            clientId: `mqtt-client-${Math.random().toString(16).substr(2, 8)}`,
            rejectUnauthorized: true,
            key: fs.readFileSync(keyPath),
            cert: fs.readFileSync(certPath),
            ca: fs.readFileSync(caPath),
            reconnectPeriod: 10000, // ⏳ AWS IoT disconnect mitigation (10s reconnection delay)
            keepalive: 60, // ⏳ AWS IoT requires keepalive to be tuned properly
        };

        this.client = connect(options.mqtt_url, mqttOptions);
        this.client.on('message', this.messageHandler.bind(this));

        this.client.on('close', () => {
            if (!this.ended) {
                this.logger.warn('⚠️ AWS IoT Disconnected');
            }
        });

        this.client.on('connect', () => {
            this.logger.info('✅ Connected to AWS IoT MQTT!');
            
            setTimeout(() => {  // ⏳ AWS IoT requires a delay before subscribing
                this.subscribeToTopics();
            }, 2000);
        });

        this.client.on('reconnect', () => {
            this.logger.info('🔄 Attempting AWS IoT Reconnect');
        });

        this.client.on('error', (error) => {
            this.logger.error('❌ AWS IoT MQTT Error:', error);
        });
    }

    async init() {
        this.logger.info('📡 Initializing AWS IoT MQTT Broker %s', this.options.mqtt_url);
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
        this.logger.debug('📴 AWS IoT MQTT Broker Deinit');
        this.ended = true;
        const cancels: Promise<void>[] = [];
        for (const { hash, channel, cb } of this.waiting) {
            this.logger.info({ hash, channel }, 'Cancelling AWS IoT Message');
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
            throw new Error('Lost connection to AWS IoT MQTT broker');
        }
    }

    async send(channel: string, payload: Buffer, options: SendOptions, ctx?: SendContext) {
        const hash = this.hasher.hash(payload);
        this.logger.debug({ hash, channel }, 'Sending AWS IoT Message %s', hash);
        await this.publish(`channel/${channel}`, payload, {
            qos: 1, // AWS IoT **only supports QoS 0 and 1**
            retain: false, // AWS IoT **does NOT support retained messages**
        });
    }

    private subscribeToTopics() {
        const topics = ['test/topic', 'buoy/data', 'buoy/status'];

        topics.forEach(topic => {
            this.client.subscribe(topic, { qos: 1 }, (error) => {
                if (error) {
                    this.logger.error(`❌ AWS IoT Subscription error: ${topic}`, error);
                } else {
                    this.logger.info(`📥 Subscribed to AWS IoT topic: ${topic}`);
                }
            });
        });
    }

    private async sendCancel(channel: string, hash?: Hash) {
        await this.publish(`channel/${channel}`, '', { qos: 1 });
        await this.publish(`cancel/${channel}`, hash?.bytes || '', { qos: 1 });
    }

    private async publish(topic: string, payload: string | Buffer, options: IClientPublishOptions = {}) {
        return new Promise<void>((resolve, reject) => {
            this.client.publish(topic, payload, options, (error) => (error ? reject(error) : resolve()));
        });
    }
}
