import requests
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

# --- Test Configuration ---
# This can be changed if your server runs on a different address
API_BASE_URL = "http://127.0.0.1:8000"
QUERY_ENDPOINT = f"{API_BASE_URL}/query"

# A simple SQL query to use for all tests.
TEST_SQL_QUERY = "SELECT 1 as id, 'test_product' as product, 100.0 as sales;"

# --- Pytest Fixtures (Optional but good practice) ---
# A fixture can provide a consistent session for all tests.
@pytest.fixture(scope="session")
def api_session():
    """Provides a requests.Session object for the tests."""
    return requests.Session()

# --- Test Functions ---

def test_json_endpoint(api_session):
    """
    Tests the endpoint to ensure it returns a valid JSON response.
    It uses pytest's 'assert' for validation.
    """
    print("\n--- 🧪 Testing JSON Endpoint ---")
    
    url = f"{QUERY_ENDPOINT}?format=json"
    payload = {"sql": TEST_SQL_QUERY}
    
    try:
        response = api_session.post(url, json=payload, timeout=10)
        
        # 1. Assert that the request was successful
        assert response.status_code == 200, f"Expected status 200, but got {response.status_code}"

        # 2. Assert that the content type is correct
        content_type = response.headers.get("content-type", "")
        assert "application/json" in content_type, f"Expected 'application/json', but got '{content_type}'"
            
        # 3. Assert that the JSON is valid and has the expected structure
        data = response.json()
        assert "result" in data, "JSON response is missing the 'result' key."
        assert isinstance(data["result"], list), "'result' key should be a list."
        
        # 4. Optional: Assert that the data is not empty and can be loaded
        df = pd.DataFrame(data['result'])
        assert not df.empty, "The returned data frame should not be empty."
        
        print("✅ Success! JSON endpoint passed all assertions.")
        print("--- Decoded JSON Data ---")
        print(df)

    except requests.exceptions.RequestException as e:
        pytest.fail(f"An error occurred during the request: {e}")

def test_arrow_endpoint(api_session):
    """
    Tests the endpoint to ensure it returns a valid Apache Arrow stream.
    """
    print("\n--- 🧪 Testing Apache Arrow Endpoint ---")
    
    url = f"{QUERY_ENDPOINT}?format=arrow"
    payload = {"sql": TEST_SQL_QUERY}
    
    try:
        response = api_session.post(url, json=payload, timeout=10)

        # 1. Assert that the request was successful
        assert response.status_code == 200, f"Expected status 200, but got {response.status_code}"

        # 2. Assert that the content type is correct for an Arrow stream
        content_type = response.headers.get("content-type", "")
        assert "application/vnd.apache.arrow.stream" in content_type, f"Expected Arrow stream content-type, but got '{content_type}'"

        # 3. Assert that the binary stream can be decoded
        try:
            with ipc.open_stream(response.content) as reader:
                arrow_table = reader.read_all()
                df = arrow_table.to_pandas()
                assert not df.empty, "Decoded Arrow stream resulted in an empty DataFrame."
                
                print("✅ Success! Arrow endpoint passed all assertions.")
                print("--- Decoded Arrow Data ---")
                print(df)

        except pa.ArrowInvalid as e:
            pytest.fail(f"Could not decode the Arrow stream: {e}")

    except requests.exceptions.RequestException as e:
        pytest.fail(f"An error occurred during the request: {e}")
