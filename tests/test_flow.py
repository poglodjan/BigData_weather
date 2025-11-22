# tests/test_flow.py
import os
import requests
import time

NAMENODE_HOST = os.environ.get('NAMENODE_HOST','namenode')
NAMENODE_PORT = os.environ.get('NAMENODE_PORT','9870')
INGEST_URL = os.environ.get('INGEST_URL','http://hdfs-ingester:5000/ingest')

sample = {
  "datetime": "2025-11-22T00:00:00Z",
  "mix": {"coal": None, "wind": 120.5, "solar": None},
  "isEstimated": False
}

def test_ingest_and_hdfs_write():
    r = requests.post(INGEST_URL, json=sample)
    assert r.status_code == 201

    body = r.json()
    assert 'path' in body

    path = body['path']
    # wait shortly for HDFS to commit
    time.sleep(1)
    open_url = f'http://{NAMENODE_HOST}:{NAMENODE_PORT}/webhdfs/v1{path}?op=OPEN'
    r2 = requests.get(open_url)
    assert r2.status_code == 200

    content = r2.text
    assert 'wind' in content

    # nulls should be replaced with 0
    assert '"coal": 0' in content or '"coal":0' in content