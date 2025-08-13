import { Broker } from './broker'
import { MqttBroker, MqttBrokerOptions } from './mqtt-broker'
import { connect } from 'mqtt'
import { v4 as uuidv4 } from 'uuid'
import { readFileContent } from '../utils'

interface MqttAWSBrokerOptions extends MqttBrokerOptions {
    mqtt_auth_key?: string
    mqtt_auth_cert?: string
    mqtt_auth_ca?: string

    mqtt_auth_folder?: string
    mqtt_auth_key_file?: string
    mqtt_auth_cert_file?: string
    mqtt_auth_ca_file?: string
}

/** Passes messages and delivery notifications over AWS IOT MQTT. */
export class MqttAWSBroker extends MqttBroker<MqttAWSBrokerOptions> implements Broker {
    protected override createClient() {
        let key = this.options.mqtt_auth_key ?? ''
        let cert = this.options.mqtt_auth_cert ?? ''
        let ca = this.options.mqtt_auth_ca ?? ''

        const CERTS_DIR = this.options.mqtt_auth_folder ?? ''

        if (!key && this.options.mqtt_auth_key_file) {
            key = readFileContent(this.options.mqtt_auth_key_file, CERTS_DIR)
        }
        if (!cert && this.options.mqtt_auth_cert_file) {
            cert = readFileContent(this.options.mqtt_auth_cert_file, CERTS_DIR)
        }
        if (!ca && this.options.mqtt_auth_ca_file) {
            ca = readFileContent(this.options.mqtt_auth_ca_file, CERTS_DIR)
        }

        if (!key || !cert || !ca) {
            throw new Error('No certificates provided for AWS MQTT authentication')
        }

        return connect(this.options.mqtt_url, {
            key,
            cert,
            ca,
            clientId: `mqtt-client-${uuidv4()}`, // TODO Check this
            rejectUnauthorized: true,
            resubscribe: false,
            clean: false, // Persistent session
            reconnectPeriod: 10000, // Increase to 10 seconds
            keepalive: 60, // Extend keepalive to prevent frequent drops
            properties: {
                sessionExpiryInterval: 3600, // 1 hour
            },
            protocolId: 'MQTT',
            protocolVersion: 5,
        })
    }
}
