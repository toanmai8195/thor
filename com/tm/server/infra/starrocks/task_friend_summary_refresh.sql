-- Cập nhật dws_friend_summary cho user có event mới (local: mỗi 1 phút; prod: 10 phút).
-- Mốc = computed_at lớn nhất của lần trước; lùi 5 phút cho event DWD nạp chậm hơn ODS.
-- Bảng rỗng → mốc 1970 → tính toàn bộ user có trong ODS (lần đầu).
SUBMIT TASK social.t_friend_summary_refresh
SCHEDULE EVERY (INTERVAL 1 MINUTE)
AS
INSERT INTO social.dws_friend_summary
SELECT user_id,
       SUM(IF(status = 'FRIEND',    1, 0)),
       SUM(IF(status = 'REQUESTED', 1, 0)),
       SUM(IF(status = 'REVIEWED',  1, 0)),
       SUM(IF(status = 'BLOCKING',  1, 0)),
       SUM(IF(status = 'BLOCKED',   1, 0)),
       MAX(last_event_time),
       NOW()
FROM social.dwd_friend_status
WHERE user_id IN (
    SELECT DISTINCT user_id FROM social.ods_friend_event
    WHERE ingest_time >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM social.dws_friend_summary) - INTERVAL 5 MINUTE
      AND event_time  >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM social.dws_friend_summary) - INTERVAL 1 DAY
)
GROUP BY user_id;
