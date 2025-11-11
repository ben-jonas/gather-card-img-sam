import json
import pytest
from pathlib import Path

def pytest_configure():
    """Make test data available globally via pytest namespace."""
    pytest.CSVUPLOAD_PAYLOADERROR_CASES = [ # type:ignore[reportAttributeAccessIssue]
        pytest.param("csvupload/payloaderror/emptyfile.json", 400, id="empty_file"),
        pytest.param("csvupload/payloaderror/emptyvalues.json", 400, id="empty_values"),
        pytest.param("csvupload/payloaderror/invalidurl.json", 400, id="invalid_url"),
        pytest.param("csvupload/payloaderror/missingcolumns.json", 400, id="missing_columns"),
        pytest.param("csvupload/payloaderror/missingheader.json", 400, id="missing_header"),
        pytest.param("csvupload/payloaderror/wrongheaders.json", 400, id="wrong_headers"),
    ]
    
    pytest.CSVUPLOAD_HAPPY_CASES = [ # type:ignore[reportAttributeAccessIssue]
        pytest.param("csvupload/happypath/csvupload-valid.json", 200, id="valid_csv"),
    ]

@pytest.fixture
def project_root():
    return Path(__file__).parent.parent

@pytest.fixture
def events_dir(project_root):
    return project_root / "events"

@pytest.fixture
def load_event(events_dir):
    def _load(filename):
        with open(events_dir / filename, 'r') as f:
            return json.load(f)
    return _load
