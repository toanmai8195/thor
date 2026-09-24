-- Kafka friend_events → DWD (upsert; chỉ ghi đè khi event_time mới >= giá trị đang có)
CREATE ROUTINE LOAD social.rl_friend_dwd ON dwd_friend_status
COLUMNS (user_id, friend_id, status, last_event_time, last_event_id, updated_at = now())
PROPERTIES (
    "format"                    = "json",
    "jsonpaths"                 = "[\"$.user_id\",\"$.friend_id\",\"$.event_type\",\"$.event_time\",\"$.event_id\"]",
    "merge_condition"           = "last_event_time",
    "desired_concurrent_number" = "1",
    "max_error_number"          = "1000"
)
FROM KAFKA (
    "kafka_broker_list"              = "kafka:9092",
    "kafka_topic"                    = "friend_events",
    "property.group.id"              = "sr_friend_dwd",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
