# Kafka Connect worker image for this example's Connect workers: the stock
# Debezium Connect image plus Aiven's S3 sink connector plugin plus
# Confluent's Avro converter. Stock Connect images ship neither, so this
# build step is required once:
#
#   docker build -t datascape-s3sink-connect:local examples/cdc-attendance/s3sink-image/
#
# Both the s3-sink Provider AND the postgres-cdc (debezium) Provider use
# this image: the parquet pipeline (docs/planning/08 D2) needs
# io.confluent.connect.avro.AvroConverter on both workers — the CDC leg to
# serialize change events as Avro against Redpanda's built-in registry, the
# sink leg to decode them into the schema-carrying records the parquet
# writer requires. The Debezium source connectors ship in the base image.
#
# Jars added (version-pinned; docs/planning/03 §7.3's worker-image
# requirement):
#   - /kafka/connect/s3-connector — Aiven s3-connector-for-apache-kafka
#     v2.15.0 (connector + its parquet writer's dependency set:
#     parquet-* 1.11.2, hadoop-common 3.3.6, kafka-connect-avro-data 7.2.2).
#   - /kafka/connect/avro-converter — Confluent kafka-connect-avro-converter
#     7.6.1 (the AvroConverter class + schema-registry client; the Aiven tar
#     bundles AvroData but not the converter itself).
FROM alpine:3.20 AS fetch
ADD https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/7.6.1/confluentinc-kafka-connect-avro-converter-7.6.1.zip /tmp/avro.zip
RUN apk add --no-cache unzip \
    && unzip -q /tmp/avro.zip -d /tmp/plugin \
    && mv /tmp/plugin/confluentinc-kafka-connect-avro-converter-*/lib /plugin

FROM quay.io/debezium/connect:2.7@sha256:f062d06e19be455ebf43cca662747f2ab6efbe4678954e7d64ac06055b8c7aff

ADD https://github.com/Aiven-Open/s3-connector-for-apache-kafka/releases/download/v2.15.0/s3-connector-for-apache-kafka-2.15.0.tar /tmp/s3-connector.tar

USER root
RUN mkdir -p /kafka/connect/s3-connector \
    && tar -xf /tmp/s3-connector.tar -C /kafka/connect/s3-connector --strip-components=1 \
    && rm /tmp/s3-connector.tar \
    && chmod -R a+rX /kafka/connect/s3-connector
COPY --from=fetch /plugin /kafka/connect/avro-converter
RUN chmod -R a+rX /kafka/connect/avro-converter
USER kafka
