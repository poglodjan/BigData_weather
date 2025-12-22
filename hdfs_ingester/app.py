from flask import Flask, request, jsonify
import os, requests, traceback
from datetime import datetime
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

# --- Configurations ---
NAMENODE_HOST = os.environ.get('NAMENODE_HOST', 'namenode')
NAMENODE_PORT = os.environ.get('NAMENODE_PORT', '9870')
DATANODE_HOST = os.environ.get('DATANODE_HOST', 'datanode')
DATANODE_PORT = os.environ.get('DATANODE_PORT', '9864')
HDFS_USER     = os.environ.get('HDFS_USER', 'hdfs')

WEBHDFS_BASE    = f'http://{NAMENODE_HOST}:{NAMENODE_PORT}/webhdfs/v1'
WEBHDFS_HEADERS = {'X-XSRF-Header': '1'}

# --- Set up session (and retry 5 times if down) ---
def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,                # Try 5 times
        backoff_factor=1,       # Wait 1s, 2s, ... 5s between trails
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def hdfs_exists(path):
    session = get_session()
    url = f"{WEBHDFS_BASE}{path}"
    r = session.get(
        url,
        params={"op": "GETFILESTATUS"},
        headers=WEBHDFS_HEADERS,
        timeout=10,
    )
    return r.status_code == 200

def hdfs_create_or_append(path, payload, method, params):
    # set up retry session
    session = get_session()

    # Step 1: Query to node (create)
    create_url = f"{WEBHDFS_BASE}{path}"

    r = session.request(
        method,
        create_url,
        params=params,
        allow_redirects=False,
        headers=WEBHDFS_HEADERS,
        timeout=60  # timeout 1 minute
    )

    final_response = None

    # Step 2: Redirect option
    if r.status_code == 307:
        redirect_url = r.headers['Location']
        u = urlparse(redirect_url)
        new_netloc = f"{DATANODE_HOST}:{DATANODE_PORT}"
        put_url = u._replace(netloc=new_netloc).geturl()

        put_headers = {
            **WEBHDFS_HEADERS,
            'Content-Type': 'application/json'
        }

        final_response = session.request(
            method,
            put_url,
            data=payload,
            headers=put_headers,
            timeout=60 
        )
    elif r.status_code == 201:
        final_response = r
    else:
        print(f"ERROR NameNode: {r.status_code} - {r.text}")
        return jsonify({'error': 'NameNode failed', 'details': r.text}), 500

    if final_response.status_code not in (200, 201):
        print(f"ERROR DataNode: {final_response.status_code} - {final_response.text}")
        return jsonify({'error': 'DataNode write failed', 'details': final_response.text}), 500

    return jsonify({'result': 'ok', 'path': path}), 201

def hdfs_create(path, payload):
    params = {
        'op': 'CREATE',
        'user.name': HDFS_USER,
        'overwrite': 'false',
    }
    print(f"INFO: Creating file {path}")
    return hdfs_create_or_append(path, payload, 'PUT', params)

def hdfs_append(path, payload):
    params = {
        'op': 'APPEND',
        'user.name': HDFS_USER,
    }
    print(f"INFO: Appending to file {path}")
    return hdfs_create_or_append(path, payload, 'POST', params)

def save_to_hdfs(path, payload):
    try:
        if not hdfs_exists(path):
            return hdfs_create(path, payload)
        # Append as next line if there is already a file for this day
        return hdfs_append(path, payload)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': 'Internal Error', 'message': str(e)}), 500


# --- ENDPOINTS /electricity and /weather ----

@app.route('/ingest/electricity/<measurement>', methods=['POST', 'HEAD'])
def ingest_electricity(measurement):
    if request.method == 'HEAD':
        return '', 200
        
    payload = request.get_json(force=True)
    if measurement == "electricity-mix":
        dt_string = payload["data"][0]["datetime"]
    else:
        dt_string = payload["datetime"]
    dt = datetime.fromisoformat(dt_string)
    day = str(dt.date())
    # File path for this day
    path = f"/user/{HDFS_USER}/electricitymaps/zone_PL/{measurement}/{day}.json"
    return save_to_hdfs(path, request.get_data() + b'\n')

@app.route('/ingest/weather/<timespan>', methods=['POST', 'HEAD'])
def ingest_weather(timespan):
    if request.method == 'HEAD':
        return '', 200

    payload = request.get_json(force=True)
    if timespan == "current":
        dt_string = payload["current"]["time"]
    else:
        dt_string = payload[timespan]["time"][0]
    dt = datetime.fromisoformat(dt_string)
    day = str(dt.date())
    # File path for this day
    path = f"/user/{HDFS_USER}/openmeteo/warsaw/{timespan}/{day}.json"
    return save_to_hdfs(path, request.get_data() + b'\n')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
