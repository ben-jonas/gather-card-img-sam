import json
import pytest
from pathlib import Path

CSVUPLOAD_HAPPY_CASES = [
    pytest.param(
        "csvupload-valid.json",
        id="happy_1"
    )
]

CSVUPLOAD_ERROR_CASES = [
    pytest.param(
        "csvupload-faulty-emptyfile.json",
        400,
        id="empty_body"
    ),
    pytest.param(
        "csvupload-faulty-missingheader.json",
        400,
        id="no_headers"
    ),
    pytest.param(
        "csvupload-faulty-wrongheaders.json",
        400,
        id="wrong_column_names"
    ),
    pytest.param(
        "csvupload-faulty-missingcolumns.json",
        400,
        id="incomplete_row"
    ),
    pytest.param(
        "csvupload-faulty-invalidurl.json",
        400,
        id="invalid_urls"
    ),
    pytest.param(
        "csvupload-faulty-emptyvalues.json",
        400,
        id="empty_fields"
    ),
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
