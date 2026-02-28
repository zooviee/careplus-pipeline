import re
import os
import boto3
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ---------- CONFIG ----------
BUCKET_NAME = "careplus-pipeline-seyi"
S3_PREFIX = "logs/"

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  os.getenv("SNOWFLAKE_DATABASE"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
}

# ---------- PARSE ----------
def parse_log_block(block):
    """Parse a single 6-line log block into a row — no transformations."""
    try:
        lines = [l.strip() for l in block.strip().split("\n") if l.strip()]
        if len(lines) < 5:
            return None

        # Line 1: timestamp, log_level, service, ticket_id, session_id
        line1 = re.match(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(.+?)\] (.+?) - TicketID=(\S+) SessionID=(\S+)",
            lines[0]
        )
        if not line1:
            return None

        timestamp  = datetime.strptime(line1.group(1), "%Y-%m-%d %H:%M:%S")
        log_level  = line1.group(2)   # raw as-is
        service    = line1.group(3)
        ticket_id  = line1.group(4)
        session_id = line1.group(5)

        # Line 2: IP, ResponseTime, CPU, EventType, Error
        line2 = re.match(
            r"IP=(\S+) \| ResponseTime=(-?\d+)ms \| CPU=(\S+)% \| EventType=(\S+) \| Error=(\S+)",
            lines[1]
        )
        if not line2:
            return None

        ip               = line2.group(1)
        response_time_ms = int(line2.group(2))   # raw as-is, negatives included
        cpu_percent      = float(line2.group(3))
        event_type       = line2.group(4)
        error            = line2.group(5)         # raw string, not converted to boolean

        # Line 3: UserAgent
        ua_match   = re.match(r'UserAgent="(.+?)"', lines[2])
        user_agent = ua_match.group(1) if ua_match else None

        # Line 5: TraceID
        trace_match = re.match(r"TraceID=(.+)", lines[4])
        trace_id    = trace_match.group(1) if trace_match else None

        return (
            timestamp, log_level, service, ticket_id, session_id,
            ip, response_time_ms, cpu_percent, event_type, error,
            user_agent, trace_id
        )

    except Exception as e:
        print(f"  ⚠️  Skipped a block due to parse error: {e}")
        return None


def parse_log_file(content):
    """Split file content into blocks and parse each one — no deduplication."""
    blocks = content.split("---")
    rows   = []

    for block in blocks:
        block = block.strip()
        if not block:
            continue
        row = parse_log_block(block)
        if row:
            rows.append(row)

    return rows


def load_to_snowflake(all_rows):
    """Bulk insert all rows into careplus.bronze.logs — skips if data already exists."""
    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    # Check if data already exists — skip if so (one-time historical load)
    cursor.execute("SELECT COUNT(*) FROM careplus.bronze.logs")
    count = cursor.fetchone()[0]
    if count > 0:
        print(f"⏭️  Skipping logs load — {count} rows already exist in bronze.logs")
        cursor.close()
        conn.close()
        return

    insert_sql = """
        INSERT INTO careplus.bronze.logs (
            timestamp, log_level, service, ticket_id, session_id,
            ip, response_time_ms, cpu_percent, event_type, error,
            user_agent, trace_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor.executemany(insert_sql, all_rows)
    conn.commit()
    cursor.close()
    conn.close()


# ---------- MAIN ----------
def main():
    s3       = boto3.client("s3")
    all_rows = []
    total_files = 0

    print("Fetching log files from S3...\n")

    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".log"):
                continue

            filename = key.split("/")[-1]
            print(f"📄 Parsing: {filename}")

            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            content  = response["Body"].read().decode("utf-8")

            rows = parse_log_file(content)
            print(f"   → {len(rows)} rows")
            all_rows.extend(rows)
            total_files += 1

    print(f"\n✅ Parsed {total_files} files — {len(all_rows)} total rows")
    print("Loading into Snowflake bronze.logs...")

    load_to_snowflake(all_rows)

    print("✅ Done! All rows loaded into careplus.bronze.logs")


if __name__ == "__main__":
    main()