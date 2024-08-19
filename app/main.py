from flask import Flask
import snowflake.connector

app = Flask(__name__)

# Snowflake connection setup
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user='MITHSU',
        password='1Lasa2baba',
        account='iykcljg-af36645',
        warehouse='COMPUTE_WH',
        database='SYSTEM_SERVICES',
        schema='ELASTICSEARCH'
    )
    return conn

@app.route('/')
def get_logs():
    conn = get_snowflake_connection()
    cur = conn.cursor()

    # Query the logs from Snowflake
    query = "SELECT log_level, message FROM APPLICATION_LOGS"
    cur.execute(query)
    logs = cur.fetchall()

    log_messages = {
        "INFO": [],
        "ERROR": [],
        "DEBUG": []
    }

    # Organize logs by log level
    for log in logs:
        log_messages[log[0]].append(log[1])

    cur.close()
    conn.close()

    # Return logs per level
    return {
        "INFO": log_messages["INFO"],
        "ERROR": log_messages["ERROR"],
        "DEBUG": log_messages["DEBUG"]
    }

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
