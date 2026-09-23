-- Chụp dws_friend_summary vào dws_friend_daily (local: mỗi 5 phút; prod: 1 lần/ngày lúc 00:05).
-- Ghi đè partition của ngày dt (dynamic_overwrite) nên chạy lại không nhân đôi.
-- dt = ngày của (NOW() - 10 phút): chạy lúc 00:05 thì ghi cho hôm qua = số cuối ngày.
SUBMIT TASK social.t_friend_daily_snapshot
SCHEDULE EVERY (INTERVAL 5 MINUTE)
AS
INSERT /*+SET_VAR(dynamic_overwrite = true)*/ OVERWRITE social.dws_friend_daily
SELECT DATE(NOW() - INTERVAL 10 MINUTE), user_id,
       friend_cnt, pending_sent_cnt, pending_received_cnt,
       blocking_cnt, blocked_cnt, last_activity, NOW()
FROM social.dws_friend_summary;
