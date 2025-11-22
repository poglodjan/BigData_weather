from flask import Flask, request, jsonify
import os, requests, json
from datetime import datetime

app = Flask(__name__)

NAMENODE_HOST = os.environ.get('NAMENODE_HOST', 'namenode')
NAMENODE_PORT = os.environ.get('NAMENODE_PORT', '9870')
HDFS_USER     = os.environ.get('HDFS_USER', 'hdfs')

WEBHDFS_BASE  = f'http://{NAMENODE_HOST}:{NAMENODE_PORT}/webhdfs/v1'
BASE_DIR      = f'/user/{HDFS_USER}/electricitymaps/zone_PL'

# Required for WebHDFS (CSRF)
WEBHDFS_HEADERS = {'X-XSRF-Header': '1'}

@app.route('/ingest', methods=['HEAD'])
def ingest_head():
    return ('', 200)

@app.route('/ingest', methods=['POST'])
def ingest():
    try:
        # Force JSON parsing — NiFi may miss Content-Type
        payload = request.get_json(force=True)

        ts   = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        path = f'{BASE_DIR}/{ts}.json'

        # Step 1 — CREATE (should return 307 redirect to DataNode)
        create_url = f"{WEBHDFS_BASE}{path}"
        params = {
            'op': 'CREATE',
            'user.name': HDFS_USER,
            'overwrite': 'true',
            'createparent': 'true'
        }

        r = requests.put(
            create_url,
            params=params,
            allow_redirects=False,
            headers=WEBHDFS_HEADERS,
            timeout=10
        )

        # --- CASE: redirect to DataNode
        if r.status_code == 307:
            put_url = r.headers['Location']
            put_headers = {
                **WEBHDFS_HEADERS,
                'Content-Type': 'application/json'
            }

            put = requests.put(
                put_url,
                data=json.dumps(payload),
                headers=put_headers,
                timeout=15
            )

            if put.status_code not in (200, 201):
                return jsonify({
                    'error': 'put failed',
                    'status': put.status_code,
                    'text': put.text
                }), 500

        # --- CASE: created without redirect
        elif r.status_code == 201:
            return jsonify({
                'result': 'ok',
                'path': path,
            }), 201

        # --- CASE: failure
        else:
            return jsonify({
                'error': 'create failed',
                'status': r.status_code,
                'text': r.text
            }), 500

        # Build return fields
        first = (payload.get('data') or [{}])[0]

        return jsonify({
            'result': 'ok',
            'path': path,
            'zone': payload.get('zone'),
            'datetime': first.get('datetime'),
            'mix': first.get('mix'),
        }), 201

    except Exception as e:
        return jsonify({
            'error': 'internal',
            'message': str(e)
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)