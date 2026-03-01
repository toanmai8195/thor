"""
=============================================================================
SEGMENT ENGINE
=============================================================================
Tính toán user segments từ gold_user_segments_base (StarRocks)
và ghi kết quả vào:

1. StarRocks segment_bitmap (RoaringBitmap):
   - Lưu bitmap của user_ids cho mỗi segment
   - Dùng cho: Fast counting, intersection, union
   - Query: bitmap_count(user_bitmap), bitmap_intersect(a, b)

2. HBase user_segments (Key-Value):
   - Lưu segments của từng user: user_id → [segment_ids]
   - Dùng cho: Real-time per-user lookup
   - Downstream: CRM, push notification, personalization

Các segments được định nghĩa:
   - seg_high_value:    LTV > $500 (90 ngày)
   - seg_active_buyer:  Mua hàng trong 30 ngày
   - seg_cart_abandoner: Add to cart, chưa mua (7 ngày)
   - seg_churned:       Từng mua, không active > 30 ngày
   - seg_new_user:      Lần đầu thấy trong 7 ngày
   - seg_loyal:         >= 3 đơn hàng
   - seg_whale:         LTV > $2000 (all time)

Dependencies (cài trong Airflow worker):
   pip install mysql-connector-python happybase
=============================================================================
"""

import os
import logging
from datetime import datetime
from typing import Dict, Set, List

import mysql.connector
import happybase

# =============================================================================
# CONFIGURATION
# =============================================================================

STARROCKS_CONFIG = {
    "host": os.getenv("STARROCKS_HOST", "starrocks-fe"),
    "port": int(os.getenv("STARROCKS_MYSQL_PORT", "9030")),
    "user": os.getenv("STARROCKS_USER", "root"),
    "password": os.getenv("STARROCKS_PASSWORD", ""),
    "database": os.getenv("STARROCKS_DB", "analytics"),
    "connect_timeout": 30,
}

HBASE_CONFIG = {
    "host": os.getenv("HBASE_THRIFT_HOST", "hbase"),
    "port": int(os.getenv("HBASE_THRIFT_PORT", "9090")),
    "timeout": 60000,  # 60 seconds
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# SEGMENT DEFINITIONS
# =============================================================================
# Mỗi segment được define bởi 1 SQL query trên gold_user_segments_base
# Query phải trả về cột "user_id" (BIGINT)
#
# Tips khi thêm segment mới:
#   1. Thêm entry vào SEGMENT_DEFINITIONS
#   2. Thêm description vào StarRocks segment_metadata (init SQL)
#   3. Test: SELECT COUNT(*) FROM gold_user_segments_base WHERE <condition>

SEGMENT_DEFINITIONS = {
    "seg_high_value": {
        "name": "High Value Users",
        "description": "LTV > $500 trong 90 ngày qua",
        # lifetime_value_90d được tính trong gold_user_segments_base
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE lifetime_value_90d > 500
            AND last_seen_days_ago <= 365
        """,
    },

    "seg_active_buyer": {
        "name": "Active Buyers",
        "description": "Đã mua hàng trong 30 ngày qua",
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE orders_30d > 0
        """,
    },

    "seg_cart_abandoner": {
        "name": "Cart Abandoners",
        "description": "Add to cart nhưng không mua trong 7 ngày qua",
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE cart_events_7d > 0
            AND orders_7d = 0
            AND last_seen_days_ago <= 7
        """,
    },

    "seg_churned": {
        "name": "Churned Users",
        "description": "Từng mua hàng nhưng không hoạt động > 30 ngày",
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE total_orders > 0
            AND last_seen_days_ago > 30
        """,
    },

    "seg_new_user": {
        "name": "New Users",
        "description": "Lần đầu thấy trong 7 ngày qua",
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE first_seen_days_ago <= 7
        """,
    },

    "seg_loyal": {
        "name": "Loyal Customers",
        "description": "Đã mua >= 3 đơn hàng",
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE total_orders >= 3
        """,
    },

    "seg_whale": {
        "name": "Whale Users",
        "description": "LTV > $2000 mọi thời gian",
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE lifetime_value > 2000
        """,
    },

    "seg_bargain_hunter": {
        "name": "Bargain Hunters",
        "description": "Hoạt động nhiều nhưng không mua (lướt tìm deal)",
        "sql": """
            SELECT user_id
            FROM gold_user_segments_base
            WHERE total_sessions > 5
            AND total_orders = 0
            AND last_seen_days_ago <= 30
        """,
    },
}


# =============================================================================
# STEP 1: COMPUTE SEGMENTS FROM STARROCKS
# =============================================================================

def compute_segments(conn: mysql.connector.MySQLConnection) -> Dict[str, Set[int]]:
    """
    Tính toán tất cả segments từ gold_user_segments_base.

    Returns:
        Dict mapping segment_id → set of user_ids
    """
    segments: Dict[str, Set[int]] = {}
    cursor = conn.cursor()

    for segment_id, segment_def in SEGMENT_DEFINITIONS.items():
        logger.info(f"Computing segment: {segment_id}")

        cursor.execute(segment_def["sql"].strip())
        user_ids = {row[0] for row in cursor.fetchall() if row[0]}

        segments[segment_id] = user_ids
        logger.info(f"  → {len(user_ids):,} users")

    cursor.close()
    return segments


# =============================================================================
# STEP 2: WRITE BITMAP TO STARROCKS
# =============================================================================

def write_bitmap_to_starrocks(
    conn: mysql.connector.MySQLConnection,
    segments: Dict[str, Set[int]]
) -> None:
    """
    Ghi bitmap segments vào StarRocks segment_bitmap table.

    StarRocks BITMAP:
    - Dùng bitmap_hash(string) để convert user_id → bitmap entry
    - AGGREGATE table tự động BITMAP_UNION khi INSERT cùng segment_id
    - Xóa data cũ trước khi INSERT để tránh tích lũy stale data

    Lý do dùng bitmap_hash thay vì dùng trực tiếp:
    - user_id là BIGINT (64-bit), BITMAP tối ưu cho 32-bit integers
    - bitmap_hash() map user_id thành 32-bit hash
    - Có thể collision nhỏ nhưng acceptable cho segmentation use case
    """
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Xóa data cũ
    logger.info("Clearing old bitmap data...")
    cursor.execute("DELETE FROM segment_bitmap WHERE 1=1")

    # Upsert segment_user_list và bitmap
    for segment_id, user_ids in segments.items():
        if not user_ids:
            logger.info(f"Skipping empty segment: {segment_id}")
            continue

        segment_name = SEGMENT_DEFINITIONS[segment_id]["name"]
        user_count = len(user_ids)

        logger.info(f"Writing bitmap for {segment_id} ({user_count:,} users)...")

        # Batch insert vào segment_user_list (flat list)
        # Dùng để export/downstream
        batch_size = 1000
        user_list = list(user_ids)

        for i in range(0, len(user_list), batch_size):
            batch = user_list[i:i + batch_size]
            values = ", ".join([
                f"('{segment_id}', {uid}, NOW(), NULL)"
                for uid in batch
            ])
            cursor.execute(f"""
                INSERT INTO segment_user_list
                    (segment_id, user_id, added_at, expires_at)
                VALUES {values}
            """)

        # Insert bitmap (StarRocks tự BITMAP_UNION)
        # Mỗi INSERT là 1 user_id → to_bitmap(user_id)
        # batch insert tất cả users của segment
        bitmap_values = ", ".join([
            f"('{segment_id}', to_bitmap({uid}), 1, '{now}')"
            for uid in user_ids
        ])
        cursor.execute(f"""
            INSERT INTO segment_bitmap
                (segment_id, user_bitmap, user_count, updated_at)
            VALUES {bitmap_values}
        """)

        logger.info(f"  → {segment_id}: bitmap written")

    conn.commit()
    cursor.close()
    logger.info("StarRocks bitmap write complete")


# =============================================================================
# STEP 3: WRITE USER→SEGMENTS TO HBASE
# =============================================================================

def write_user_segments_to_hbase(
    segments: Dict[str, Set[int]]
) -> None:
    """
    Ghi user → segments mapping vào HBase.

    HBase Schema:
    - Table: user_segments
    - Row key: str(user_id)  e.g., "12345"
    - Column family: "seg"
    - Qualifier: segment_id  e.g., "seg_high_value"
    - Value: ISO timestamp  e.g., "2024-01-15T10:30:00"

    Ví dụ HBase row:
      Row: "12345"
        seg:seg_high_value    → "2024-01-15T10:30:00"
        seg:seg_active_buyer  → "2024-01-15T10:30:00"
        seg:seg_loyal         → "2024-01-15T10:30:00"

    Tại sao HBase?
    - O(1) point lookup: GET user_segments "12345" → tất cả segments
    - Scale tốt: hàng triệu users
    - Downstream: Real-time personalization, push notification targeting

    happybase: Python client cho HBase Thrift Server (port 9090)
    """
    logger.info("Connecting to HBase...")

    # Build reverse mapping: user_id → [segment_ids]
    user_to_segments: Dict[int, List[str]] = {}
    for segment_id, user_ids in segments.items():
        for user_id in user_ids:
            if user_id not in user_to_segments:
                user_to_segments[user_id] = []
            user_to_segments[user_id].append(segment_id)

    logger.info(f"Writing {len(user_to_segments):,} users to HBase...")

    connection = happybase.Connection(
        host=HBASE_CONFIG["host"],
        port=HBASE_CONFIG["port"],
        timeout=HBASE_CONFIG["timeout"],
    )

    try:
        table = connection.table("user_segments")
        now_str = datetime.now().isoformat()

        # Dùng batch để tránh nhiều round trips
        # batch_size = số mutations (put/delete) trước khi flush
        with table.batch(batch_size=500) as batch:
            for user_id, user_segments in user_to_segments.items():
                row_key = str(user_id).encode("utf-8")

                # Mỗi segment = 1 column trong family "seg"
                # {b"seg:segment_id": b"timestamp"}
                row_data = {
                    f"seg:{seg_id}".encode("utf-8"): now_str.encode("utf-8")
                    for seg_id in user_segments
                }

                batch.put(row_key, row_data)

        # Cũng ghi vào segment_members_reverse table
        # (optional reverse index: segment → users)
        reverse_table = connection.table("segment_members_reverse")
        with reverse_table.batch(batch_size=500) as batch:
            for segment_id, user_ids in segments.items():
                if not user_ids:
                    continue

                row_key = segment_id.encode("utf-8")
                row_data = {
                    f"users:{uid}".encode("utf-8"): now_str.encode("utf-8")
                    for uid in user_ids
                }
                batch.put(row_key, row_data)

        logger.info(f"HBase write complete: {len(user_to_segments):,} users")

    finally:
        connection.close()


# =============================================================================
# MAIN
# =============================================================================

def run_segment_engine(**context) -> Dict[str, int]:
    """
    Chạy toàn bộ segment engine pipeline.
    Đây là entry point được Airflow PythonOperator gọi.

    Returns:
        Dict với segment sizes: {segment_id: user_count}
    """
    logger.info("=== Segment Engine Starting ===")

    # Connect to StarRocks
    logger.info("Connecting to StarRocks...")
    sr_conn = mysql.connector.connect(**STARROCKS_CONFIG)

    try:
        # Step 1: Compute segments từ Gold layer
        logger.info("Step 1: Computing segments from gold_user_segments_base")
        segments = compute_segments(sr_conn)

        # Log segment sizes
        segment_sizes = {seg_id: len(users) for seg_id, users in segments.items()}
        logger.info(f"Segment sizes: {segment_sizes}")

        # Step 2: Write bitmap → StarRocks
        logger.info("Step 2: Writing bitmaps to StarRocks")
        write_bitmap_to_starrocks(sr_conn, segments)

    finally:
        sr_conn.close()

    # Step 3: Write user→segments → HBase
    logger.info("Step 3: Writing user segments to HBase")
    try:
        write_user_segments_to_hbase(segments)
    except Exception as e:
        # HBase failure không nên block pipeline
        # StarRocks bitmap đã được ghi thành công
        logger.error(f"HBase write failed (non-fatal): {e}")

    logger.info("=== Segment Engine Complete ===")
    return segment_sizes


if __name__ == "__main__":
    result = run_segment_engine()
    for seg_id, count in sorted(result.items(), key=lambda x: -x[1]):
        print(f"  {seg_id}: {count:,} users")
