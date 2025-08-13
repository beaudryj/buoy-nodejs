# buoy

POST -> WebSocket forwarder

## Usage

-   POST to any data to `/<uuid>`
-   UPGRADE a WebSocket on `/<uuid>` to receive data

### Headers

`X-Buoy-Delivery` will be set to `delivered` if the data was delivered directly to a listening socket, otherwise `buffered`.

`X-Buoy-Wait` can be specified in number of seconds to wait for the data to be delivered to a listener, will return a `408` if the data cannot be delivered within time limit. Will always have `X-Buoy-Delivery: delivered` on a successful request.

`X-Buoy-Soft-Wait` can be specified in number of seconds to wait for the data to be delivered to a listener, will return a `202` if the data cannot be delivered within time limit.

## Run with docker

```
$ docker build .
...
<container id>
$ docker run -d --name buoy <container id>
```

## Environment variables
`PORT` - port to app to listen
`NUM_WORKERS` - number of workers to start for the app. Default: 1

`ACT_AS_PROXY` - flag to start service in a proxy mode when requests will be sent to and listened from `cb.acnhor.link`

`BROKER_MQTT_URL` - a url of the MQTT broker

`mqtt_aws` broker instance requires TLS connection with authentication. Private key and ceritificates are required in this case.

`BROKER_MQTT_AUTH_KEY` - a private key for `mqtts:` authentication
`BROKER_MQTT_AUTH_CERT` - a certificate for `mqtts:` authentication
`BROKER_MQTT_AUTH_CA` - a CA certificate for `mqtts:` authentication

Another option is to provide no the file content, but the file paths. In this case the application will get the content on its own.

`BROKER_MQTT_AUTH_KEY_FILE` - a path to the private key file. Optional.
`BROKER_MQTT_AUTH_CERT_FILE` - a path to the certificate file. Optional.
`BROKER_MQTT_AUTH_CA_FILE` - a path to the CA ceritificate file. Optional.

`BROKER_MQTT_AUTH_FOLDER` - a root folder for certificates and private key. Optional. Default: current app folder.

## Development

`Make` is requred to build and develop the project.

To do local development do the following:
1. Clone the report:
    ```
    git clone https://github.com/greymass/buoy-nodejs.git
    cd buoy-nodejs
    ```
2. Install dependencies: `yarn`
3. Start local server: `make dev`
4. You can run different configurations using `NODE_CONFIG_ENV` variable. For example to run AWS ready service: `NODE_CONFIG_ENV=aws make dev`.
5. To provide an environment variable you can just define it in a command line. For example to run AWS ready service in a proxy mode:
`ACT_AS_PROXY=true NODE_CONFIG_ENV=aws make dev`
