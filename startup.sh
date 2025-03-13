#!/bin/sh

# Create a temporary directory for the certificate files
CERTS_DIR="/tmp/certs"
mkdir -p $CERTS_DIR

# Write environment variables to temporary files
echo "$IOT_PRIVATE_KEY" > $CERTS_DIR/private.key
echo "$IOT_CERTIFICATE" > $CERTS_DIR/certificate.pem
echo "$AWS_IOT_CA_CERT" > $CERTS_DIR/AmazonRootCA1.pem

# Start the Node.js application
exec node lib/app.js