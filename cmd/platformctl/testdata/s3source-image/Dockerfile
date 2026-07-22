# Kafka Connect worker image for the s3source provider (docs/planning/08
# D4): the stock Debezium Connect image ships no S3 source plugin, so
# spec.configuration.image is required and must contain the Aiven S3
# source connector on the plugin path (mirrors s3sink's testdata Dockerfile
# pattern). Built by cmd/platformctl/s3source_integration_test.go as
# datascape-s3source-connect:test.
#
# Jars this image adds, and why:
#   - /kafka/connect/s3-source-connector — Aiven's
#     s3-source-connector-for-apache-kafka v3.4.2 release tar, from the
#     Aiven-Open/cloud-storage-connectors-for-apache-kafka repository. The
#     repo s3sink's own plugin comes from (Aiven-Open/s3-connector-for-
#     apache-kafka) was archived 2024-09-11 with development moved here;
#     this is this platform's first consumer of the new repo. The tar
#     bundles its own Confluent Avro converter (kafka-connect-avro-
#     converter 7.2.2) and parquet/hadoop jars, so avro/parquet-format
#     ingest needs no extra plugin jar (unlike s3sink/avro-connect-image,
#     which had to add the converter separately).
FROM quay.io/debezium/connect:2.7@sha256:f062d06e19be455ebf43cca662747f2ab6efbe4678954e7d64ac06055b8c7aff

ADD https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/releases/download/v3.4.2/s3-source-connector-for-apache-kafka-3.4.2.tar /tmp/s3-source-connector.tar

USER root
RUN mkdir -p /kafka/connect/s3-source-connector \
    && tar -xf /tmp/s3-source-connector.tar -C /kafka/connect/s3-source-connector --strip-components=1 \
    && rm /tmp/s3-source-connector.tar \
    && chmod -R a+rX /kafka/connect/s3-source-connector
USER kafka
