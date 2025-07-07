
import psycopg2

def get_connection():
    return psycopg2.connect(
        host='your_host',
        port='5432',
        database='your_db',
        user='your_user',
        password='your_password'
    )

def update_merchant_txn_count(conn, merchant_id, increment=1):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO merchant_txn_count (merchant_id, txn_count)
        VALUES (%s, %s)
        ON CONFLICT (merchant_id)
        DO UPDATE SET txn_count = merchant_txn_count.txn_count + %s;
    """, (merchant_id, increment, increment))
    conn.commit()
    cur.close()

def mark_pattern_detected(conn, pattern_id, customer_id, merchant_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO detected_patterns (pattern_id, customer_id, merchant_id)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (pattern_id, customer_id, merchant_id))
    conn.commit()
    cur.close()
