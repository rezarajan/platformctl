# Kafka Connect worker image for the s3sink provider integration test:
# the stock Debezium Connect image plus Aiven's S3 sink connector plugin.
# Built by cmd/platformctl/sink_integration_test.go as datascape-s3sink-connect:test.
FROM quay.io/debezium/connect:2.7

ADD https://github.com/Aiven-Open/s3-connector-for-apache-kafka/releases/download/v2.15.0/s3-connector-for-apache-kafka-2.15.0.tar /tmp/s3-connector.tar

USER root
RUN mkdir -p /kafka/connect/s3-connector \
    && tar -xf /tmp/s3-connector.tar -C /kafka/connect/s3-connector --strip-components=1 \
    && rm /tmp/s3-connector.tar \
    && chmod -R a+rX /kafka/connect/s3-connector
USER kafka
