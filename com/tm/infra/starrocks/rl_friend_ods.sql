-- Kafka friend_events → ODS (append)
CREATE ROUTINE LOAD social.rl_friend_ods ON ods_friend_event
COLUMNS (user_id, friend_id, event_time, event_type, event_id, source, ingest_time = now())
PROPERTIES (
    "format"                    = "json",
    "jsonpaths"                 = "[\"$.user_id\",\"$.friend_id\",\"$.event_time\",\"$.event_type\",\"$.event_id\",\"$.source\"]",
    "desired_concurrent_number" = "1",
    "max_error_number"          = "1000"
)
FROM KAFKA (
    "kafka_broker_list"              = "kafka:9092",
    "kafka_topic"                    = "friend_events",
    "property.group.id"              = "sr_friend_ods",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
