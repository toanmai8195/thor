#!/bin/bash
# =============================================================================
# HBASE TABLE INITIALIZATION
# =============================================================================
# Script này tạo các HBase tables cần thiết cho Segment Engine.
#
# HBase Data Model:
#   - Table: user_segments
#   - Row key: user_id (e.g., "12345")
#   - Column family: "seg" (short for segments)
#   - Column qualifiers: segment_id (e.g., "seg_high_value")
#   - Value: timestamp khi user được assign vào segment
#
# Ví dụ:
#   Row key: "12345"
#   seg:seg_high_value    → "2024-01-15T10:30:00"
#   seg:seg_active_buyer  → "2024-01-15T10:30:00"
#
# Access Pattern:
#   GET user_segments, "12345"                → Tất cả segments của user 12345
#   GET user_segments, "12345", "seg:seg_high_value"  → Check 1 segment cụ thể
#   SCAN user_segments                        → All users (dùng sparingly)
#
# Tại sao HBase?
#   - O(1) point lookup theo user_id
#   - Scale horizontal (rows = users, có thể có hàng triệu)
#   - Column family giúp cache hot data (chỉ read "seg" family)
#   - Downstream systems (CRM, push notification) dễ dùng
#
# Python integration: happybase library (Thrift protocol, port 9090)
# =============================================================================

set -e

echo "=== HBase Table Initialization ==="
echo "Waiting for HBase to be fully started..."

# Wait cho HBase sẵn sàng
MAX_RETRIES=30
RETRY_COUNT=0

while ! echo status | hbase shell 2>/dev/null | grep -q "servers"; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: HBase not ready after ${MAX_RETRIES} retries"
        exit 1
    fi
    echo "Waiting for HBase... (attempt $RETRY_COUNT/$MAX_RETRIES)"
    sleep 10
done

echo "HBase is ready. Creating tables..."

# =============================================================================
# Create tables using HBase Shell
# =============================================================================
hbase shell << 'HBASE_SCRIPT'

# ==========================================================================
# TABLE: user_segments
# ==========================================================================
# Lưu user → segments mapping
# Row key: user_id (string)
# Column family: seg (segment memberships)
# Column family: meta (user metadata - optional)
#
# Column family settings:
#   VERSIONS => 1: Chỉ giữ 1 version (latest)
#   COMPRESSION => SNAPPY: Nén data (~50% size reduction)
#   BLOOMFILTER => ROW: Filter cho point lookups
#   TTL => 7776000: 90 days (seconds), tự động expire
# ==========================================================================
create 'user_segments',
  {NAME => 'seg',
   VERSIONS => 1,
   COMPRESSION => 'SNAPPY',
   BLOOMFILTER => 'ROW',
   TTL => 7776000},
  {NAME => 'meta',
   VERSIONS => 1,
   COMPRESSION => 'SNAPPY',
   BLOOMFILTER => 'ROW',
   TTL => 7776000}

# ==========================================================================
# TABLE: segment_members_reverse
# ==========================================================================
# Reverse index: segment_id → user_ids
# Dùng khi cần list tất cả users của một segment (ít dùng hơn)
#
# Row key: segment_id (e.g., "seg_high_value")
# Column family: users (column = user_id, value = timestamp)
#
# Lưu ý: Table này sẽ có ít rows (= số segments)
# nhưng mỗi row có rất nhiều columns (= số users trong segment)
# Dùng COMPRESSED để tiết kiệm space
# ==========================================================================
create 'segment_members_reverse',
  {NAME => 'users',
   VERSIONS => 1,
   COMPRESSION => 'SNAPPY',
   BLOOMFILTER => 'ROWCOL',
   TTL => 7776000}

# Verify tables created successfully
list

# Show table descriptions
describe 'user_segments'
describe 'segment_members_reverse'

puts "HBase tables created successfully!"

HBASE_SCRIPT

echo "=== HBase initialization complete ==="
echo ""
echo "Tables created:"
echo "  - user_segments: User → Segments mapping (primary lookup)"
echo "  - segment_members_reverse: Segment → Users reverse index"
echo ""
echo "Access via:"
echo "  Python: happybase.Connection(host='hbase', port=9090)"
echo "  Web UI: http://localhost:16010"
echo "  REST:   http://localhost:8080"
