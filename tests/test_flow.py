# tests:
# test_create_with_redirect - verifies the full redirect scenario where WebHDFS responds with 307 and data is successfully written to the DataNode.
# test_create_without_redirect - ensures the application correctly handles the case where WebHDFS creates the file with status 201.
# test_invalid_json - Checks how the application responds when the incoming request contains non-JSON data
# test_flow - Validating the entire ingestion flow and confirming the returned fields from the API are correct.

import json
import pytest
from unittest.mock import patch, MagicMock
from hdfs_ingester import app  

@pytest.fixture
def client():
    app.testing = True
    return app.test_client()

def sample_payload():
    return {
        "zone": "PL",
        "temporalGranularity": "5_minutes",
        "unit": "MW",
        "data": [
            {
                "datetime": "2025-11-22T17:50:00.000Z",
                "updatedAt": "2025-11-22T17:42:42.266Z",
                "isEstimated": True,
                "estimationMethod": "ESTIMATED_TIME_SLICER_AVERAGE",
                "mix": {
                    "nuclear": 219,
                    "geothermal": 1,
                    "biomass": 533,
                    "coal": 11934,
                    "wind": 2536,
                    "solar": 0,
                    "hydro": 535,
                    "gas": 4033,
                    "oil": 236,
                    "unknown": 751,
                    "hydro discharge": 563,
                    "battery discharge": 1
                }
            }
        ]
    }


# ----------------------------------------------------------
# 1) TEST: WebHDFS returns 307 → DataNode PUT → OK
# ----------------------------------------------------------
def test_create_with_redirect(client, mocker):
    payload = sample_payload()

    mock_put = mocker.patch("requests.put")

    # first call: 307 redirect
    first = MagicMock()
    first.status_code = 307
    first.headers = {'Location': 'http://datanode/upload'}

    # second call: data write
    second = MagicMock()
    second.status_code = 201

    mock_put.side_effect = [first, second]

    response = client.post("/ingest", json=payload)
    data = response.get_json()

    assert response.status_code == 201
    assert data["result"] == "ok"
    assert data["zone"] == "PL"
    assert data["datetime"] == payload["data"][0]["datetime"]
    assert data["mix"] == payload["data"][0]["mix"]

    # verifies that PUT was called 2x
    assert mock_put.call_count == 2


# ----------------------------------------------------------
# 2) TEST: WebHDFS calls 201 without redirectu
# ----------------------------------------------------------
def test_create_without_redirect(client, mocker):
    payload = sample_payload()

    mock_put = mocker.patch("requests.put")

    # WebHDFS: 201 immediatly
    first = MagicMock()
    first.status_code = 201
    mock_put.return_value = first

    response = client.post("/ingest", json=payload)
    data = response.get_json()

    assert response.status_code == 201
    assert data["result"] == "ok"
    assert "zone" in data  
    assert mock_put.call_count == 1


# ----------------------------------------------------------
# 3) TEST: invalid json?
# ----------------------------------------------------------
def test_invalid_json(client):
    response = client.post("/ingest", data="not-json")
    assert response.status_code == 500
    assert response.get_json()["error"] == "internal"


# ----------------------------------------------------------
# 4) TEST FLOW – entire simulation end-to-end
# ----------------------------------------------------------
def test_flow(client, mocker):
    """
    scenario:
    - WebHDFS first PUT → 307 redirect
    - second PUT → 201
    - check if API correctly returns:
      zone, datetime, mix, path
    """

    payload = sample_payload()

    mock_put = mocker.patch("requests.put")

    first = MagicMock()
    first.status_code = 307
    first.headers = {'Location': 'http://datanode/upload'}

    second = MagicMock()
    second.status_code = 201

    mock_put.side_effect = [first, second]

    response = client.post("/ingest", json=payload)
    data = response.get_json()

    assert response.status_code == 201
    assert data["result"] == "ok"
    assert data["zone"] == "PL"
    assert data["datetime"] == payload["data"][0]["datetime"]
    assert data["mix"] == payload["data"][0]["mix"]
    assert data["path"].startswith("/user/hdfs/electricitymaps/zone_PL/")

    assert mock_put.call_count == 2
