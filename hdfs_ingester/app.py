from flask import Flask, request, jsonify
import os, requests, json, traceback
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

# --- utility function (saving to particular target dir) ---
def save_to_hdfs(payload, target_dir):
    try:
        if not payload:
            return jsonify({'error': 'Empty payload'}), 400

        ts   = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        path = f'{target_dir}/{ts}.json'

        print(f"INFO: Saving to {path}")

        # set up retry session
        session = get_session()

        # Step 1: Query to node (create)
        create_url = f"{WEBHDFS_BASE}{path}"
        params = {
            'op': 'CREATE',
            'user.name': HDFS_USER,
            'overwrite': 'true',
            'createparent': 'true'
        }

        r = session.put(
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

            final_response = session.put(
                put_url,
                data=json.dumps(payload),
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

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': 'Internal Error', 'message': str(e)}), 500


# --- ENDPOINTS /electricity and /weather ----

@app.route('/ingest/electricity', methods=['POST', 'HEAD'])
def ingest_electricity():
    if request.method == 'HEAD':
        return '', 200
        
    target_dir = f'/user/{HDFS_USER}/electricitymaps/zone_PL'
    payload = request.get_json(force=True)
    return save_to_hdfs(payload, target_dir)

@app.route('/ingest/weather', methods=['POST', 'HEAD'])
def ingest_weather():
    if request.method == 'HEAD':
        return '', 200

    target_dir = f'/user/{HDFS_USER}/openmeteo/warsaw'
    payload = request.get_json(force=True)
    return save_to_hdfs(payload, target_dir)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)