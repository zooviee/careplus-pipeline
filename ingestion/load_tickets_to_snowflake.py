import os
import csv
import boto3
import mysql.connector
import snowflake.connector
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

# ---------- CONFIG ----------
BUCKET_NAME = "careplus-pipeline-seyi"
S3_KEY      = "tickets/support_tickets.csv"

MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "database": os.getenv("MYSQL_DATABASE"),
    "user":     os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  os.getenv("SNOWFLAKE_DATABASE"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA"),
}

# ---------- GET LAST LOADED TIMESTAMP ----------
def get_last_loaded_timestamp():
    """Get the max created_at already in Snowflake bronze.tickets."""
    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(created_at) FROM careplus.bronze.tickets")
    result = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if result:
        print(f"📅 Last loaded timestamp: {result}")
    else:
        print("📅 No existing data — performing full load")

    return result


# ---------- EXTRACT FROM MYSQL ----------
def extract_from_mysql(last_loaded_at):
    print("Connecting to MySQL...")
    conn   = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    if last_loaded_at:
        last_loaded_at = last_loaded_at.strftime("%Y-%m-%d %H:%M")

        query = """
        SELECT *
        FROM support_tickets
        WHERE created_at > %s
        ORDER BY created_at
        """
        cursor.execute(query, (last_loaded_at,))
    else:
        cursor.execute("SELECT * FROM support_tickets")

    rows    = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    print(f"✅ Extracted {len(rows)} new rows from MySQL")
    return columns, rows


# ---------- UPLOAD TO S3 ----------
def upload_to_s3(columns, rows):
    if not rows:
        print("⏭️  No new rows to upload to S3")
        return False

    print("Uploading to S3...")
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows(rows)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_KEY,
        Body=buffer.getvalue().encode("utf-8")
    )

    print(f"✅ Uploaded to s3://{BUCKET_NAME}/{S3_KEY}")
    return True


# ---------- LOAD FROM S3 TO SNOWFLAKE ----------
def load_to_snowflake():
    print("Loading into Snowflake bronze.tickets...")

    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE STAGE IF NOT EXISTS careplus.bronze.s3_stage
        URL = 's3://careplus-pipeline-seyi/'
        CREDENTIALS = (
            AWS_KEY_ID     = '{aws_key}'
            AWS_SECRET_KEY = '{aws_secret}'
        )
        FILE_FORMAT = (
            TYPE             = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER      = 1
            NULL_IF          = ('NULL', 'null', '')
        )
    """.format(
        aws_key    = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    ))

    cursor.execute("""
        COPY INTO careplus.bronze.tickets (
            ticket_id, created_at, resolved_at, agent,
            priority, num_interactions, issue_cat,
            channel, status, agent_feedback
        )
        FROM @careplus.bronze.s3_stage/tickets/support_tickets.csv
        FILE_FORMAT = (
            TYPE             = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER      = 1
            NULL_IF          = ('NULL', 'null', '')
        )
        ON_ERROR = CONTINUE
    """)

    results = cursor.fetchall()
    for row in results:
        print(f"   {row}")

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Done! New tickets loaded into careplus.bronze.tickets")


# ---------- MAIN ----------
def main():
    last_loaded_at        = get_last_loaded_timestamp()
    columns, rows         = extract_from_mysql(last_loaded_at)

    uploaded = upload_to_s3(columns, rows)
    if uploaded:
        load_to_snowflake()
    else:
        print("✅ Nothing to load — Snowflake is already up to date")


if __name__ == "__main__":
    main()