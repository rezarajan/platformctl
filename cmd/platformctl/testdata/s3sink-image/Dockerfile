# Kafka Connect worker image for the s3sink provider integration tests:
# the stock Debezium Connect image plus Aiven's S3 sink connector plugin.
# Built by cmd/platformctl/sink_integration_test.go (and the parquet D2
# suite, parquet_sink_integration_test.go) as datascape-s3sink-connect:test.
#
# Jars this image adds, and why (docs/planning/08 D2):
#   - /kafka/connect/s3-connector — Aiven's s3-connector-for-apache-kafka
#     v2.15.0 release tar. Ships the connector itself AND its parquet
#     writer's full dependency set (parquet-avro/-hadoop/-column/... 1.11.2,
#     hadoop-common 3.3.6, kafka-connect-avro-data 7.2.2), so
#     format.output.type=parquet needs no extra format jars.
#   - /kafka/connect/avro-converter — Confluent's kafka-connect-avro-converter
#     7.6.1 (io.confluent.connect.avro.AvroConverter + registry client). The
#     Aiven tar bundles AvroData but NOT the converter class, and the stock
#     Debezium image ships Apicurio converters only — yet parquet output
#     requires schema-carrying records, which arrive Avro-encoded against
#     Redpanda's Confluent-compatible registry. Same version-pinned pattern
#     as testdata/avro-connect-image (docs/planning/03 §7.3's worker-image
#     requirement).
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
