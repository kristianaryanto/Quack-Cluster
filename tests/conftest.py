# tests/conftest.py

import pytest
from fastapi.testclient import TestClient
from quack_cluster.coordinator import app

@pytest.fixture(scope="module")
def api_client():
    """
    Membuat instance TestClient untuk berinteraksi dengan API FastAPI.
    'scope="module"' berarti klien ini akan dibuat sekali per file tes.
    """
    with TestClient(app) as client:
        yield client