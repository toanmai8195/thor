#!/bin/sh
# Khởi tạo StarRocks local: schema + Routine Load + task. Chạy lại nhiều lần được.
set -eu
SQL="mysql -h starrocks -P 9030 -uroot --connect-timeout=10"
q() { $SQL -N -e "$1" 2>/dev/null || true; }

# Nâng cấp từ thiết kế cũ (dữ liệu DWS local tính lại được từ DWD)
if q "SHOW CREATE TABLE social.dws_friend_summary" | grep -q "MATERIALIZED VIEW"; then
  $SQL -e "DROP MATERIALIZED VIEW social.dws_friend_summary"; echo "xoá MV dws_friend_summary cũ"
fi
if q "SHOW CREATE TABLE social.dws_friend_daily" | grep -q "PRIMARY KEY"; then
  $SQL -e "DROP TABLE social.dws_friend_daily"; echo "xoá dws_friend_daily kiểu cũ"
fi

$SQL < /init/01_schema.sql
echo "schema ok"

# Routine Load không có IF NOT EXISTS → chỉ tạo khi chưa có job
for rl in rl_friend_ods rl_friend_dwd; do
  if q "SHOW ROUTINE LOAD FOR social.$rl" | grep -q "$rl"; then
    echo "$rl đã có"
  else
    $SQL < "/init/$rl.sql"; echo "tạo $rl"
  fi
done

# Task không lưu dữ liệu → luôn tạo lại để cập nhật theo file
for t in t_friend_summary_refresh t_friend_daily_snapshot; do
  if q "SELECT TASK_NAME FROM information_schema.tasks WHERE TASK_NAME='$t'" | grep -q .; then
    $SQL -e "DROP TASK $t"
  fi
  $SQL < "/init/task_$(echo $t | sed 's/^t_//').sql" > /dev/null; echo "tạo task $t"
done

$SQL -e "SHOW ROUTINE LOAD FROM social\G" | grep -E "Name:|State:"
$SQL -e "SELECT TASK_NAME, SCHEDULE FROM information_schema.tasks"
