# Kafka Connect worker image for the Avro CDC integration test
# (docs/planning/08 D1): the stock Debezium Connect image ships only the
# Apicurio converter jars, not Confluent's AvroConverter — but Redpanda's
# built-in schema registry speaks the Confluent API, so the provider wires
# io.confluent.connect.avro.AvroConverter and the worker image must carry
# it. Same pattern as testdata/s3sink-image (stock image lacks the plugin;
# a version-pinned testdata build adds it).
# Built by cmd/platformctl/avro_cdc_integration_test.go as
# datascape-avro-connect:test.
FROM alpine:3.20 AS fetch
ADD https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/7.6.1/confluentinc-kafka-connect-avro-converter-7.6.1.zip /tmp/avro.zip
RUN apk add --no-cache unzip \
    && unzip -q /tmp/avro.zip -d /tmp/plugin \
    && mv /tmp/plugin/confluentinc-kafka-connect-avro-converter-*/lib /plugin

FROM quay.io/debezium/connect:2.7@sha256:f062d06e19be455ebf43cca662747f2ab6efbe4678954e7d64ac06055b8c7aff
USER root
COPY --from=fetch /plugin /kafka/connect/avro-converter
RUN chmod -R a+rX /kafka/connect/avro-converter
USER kafka
