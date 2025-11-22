
from flask import Flask, request, jsonify
import os
import requests
import json
from datetime import datetime

app = Flask(__name__)

NAMENODE_HOST = os.environ.get('NAMENODE_HOST', 'namenode')
NAMENODE_PORT = os.environ.get('NAMENODE_PORT', '9870')
HDFS_USER = os.environ.get('HDFS_USER', 'hdfs')

WEBHDFS_BASE = f'http://{NAMENODE_HOST}:{NAMENODE_PORT}/webhdfs/v1'


@app.route('/ingest', methods=['POST'])
def ingest():
    payload = request.get_json()
    if payload is None:
        return jsonify({'error': 'no json'}), 400

    # Create name of the file (datetime)
    ts = datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    path = f'/electricitymaps/zone_PL/{ts}.json'

    # Create new file in HDFS
    params = {'op': 'CREATE', 'user.name': HDFS_USER, 'overwrite': 'true'}
    create_url = f"{WEBHDFS_BASE}{path}"
    r = requests.put(create_url, params=params, allow_redirects=False)

    # Check for errors
    if r.status_code not in (307, 201):
        return jsonify({'error': 'create failed'}), 500

    # Take this URL for PUT
    put_url = r.headers.get('Location')
    if put_url:
        requests.put(put_url, data=json.dumps(payload))

    # Data that we got
    return jsonify({
        'result': 'ok',
        'path': path,
        'zone': payload.get('zone'),
        'datetime': payload.get('data', [{}])[0].get('datetime'),
        'mix': payload.get('data', [{}])[0].get('mix')
    }), 201


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
