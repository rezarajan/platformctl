# Kafka Connect worker image for the jdbcsink provider (docs/planning/08
# D3): the stock Debezium Connect image ships no JDBC sink plugin, so
# spec.configuration.image is required and must contain Confluent's JDBC
# sink connector on the plugin path (mirrors s3sink's testdata Dockerfile
# pattern: stock image lacks the plugin -> a version-pinned testdata build
# adds it). Built by cmd/platformctl/jdbcsink_integration_test.go as
# datascape-jdbcsink-connect:test.
#
# jdbcsink requires schema-carrying records (avro/protobuf,
# jdbcsink.ValidateBindingOptions) — kafka-connect-jdbc's own
# FieldsMetadata.extract derives value columns and pk.mode=record_key only
# from a Struct-typed schema, so the Confluent Avro converter is added
# alongside the JDBC plugin, not optional.
#
# Jars this image adds, and why:
#   - /kafka/connect/kafka-connect-jdbc — Confluent's kafka-connect-jdbc
#     v10.9.6 Confluent Hub release zip. Ships the connector itself
#     (io.confluent.connect.jdbc.JdbcSinkConnector) and, bundled by
#     Confluent, the PostgreSQL JDBC driver (postgresql-42.7.11.jar) —
#     sufficient for postgres sink targets with no extra jar. MySQL is NOT
#     bundled (license): mysql-connector-j 9.7.0 is added alongside it in
#     the same plugin directory so both engines this provider declares
#     (SupportedSinkEngines: postgres, mysql) work from one image.
#   - /kafka/connect/avro-converter — Confluent's
#     kafka-connect-avro-converter 7.6.1 (io.confluent.connect.avro.
#     AvroConverter + registry client) — the same version-pinned jar
#     testdata/avro-connect-image and testdata/s3sink-image add, for the
#     identical reason (docs/planning/03 §7.3's worker-image requirement):
#     the stock Debezium image ships Apicurio converters only, not
#     Confluent's, and Redpanda's built-in registry speaks the Confluent
#     API.
FROM alpine:3.20 AS fetch
ADD https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/7.6.1/confluentinc-kafka-connect-avro-converter-7.6.1.zip /tmp/avro.zip
ADD https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.9.6/confluentinc-kafka-connect-jdbc-10.9.6.zip /tmp/jdbc.zip
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.7.0/mysql-connector-j-9.7.0.jar /jdbc-plugin/mysql-connector-j-9.7.0.jar
RUN apk add --no-cache unzip \
    && unzip -q /tmp/avro.zip -d /tmp/avro-plugin \
    && mkdir -p /avro-plugin \
    && mv /tmp/avro-plugin/confluentinc-kafka-connect-avro-converter-*/lib/* /avro-plugin/ \
    && unzip -q /tmp/jdbc.zip -d /tmp/jdbc-plugin-src \
    && mv /tmp/jdbc-plugin-src/confluentinc-kafka-connect-jdbc-*/lib/* /jdbc-plugin/

FROM quay.io/debezium/connect:2.7@sha256:f062d06e19be455ebf43cca662747f2ab6efbe4678954e7d64ac06055b8c7aff

USER root
COPY --from=fetch /avro-plugin /kafka/connect/avro-converter
COPY --from=fetch /jdbc-plugin /kafka/connect/kafka-connect-jdbc
RUN chmod -R a+rX /kafka/connect/avro-converter /kafka/connect/kafka-connect-jdbc
USER kafka
