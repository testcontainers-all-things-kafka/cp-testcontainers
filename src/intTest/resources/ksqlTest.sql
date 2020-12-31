CREATE STREAM source_json (registertime BIGINT, userid VARCHAR, regionid VARCHAR, gender VARCHAR)
WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON');
CREATE STREAM target_avro WITH (KAFKA_TOPIC='users_avro', VALUE_FORMAT='AVRO') AS
SELECT * FROM source_json;
