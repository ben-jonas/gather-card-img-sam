import pytest

import sys
print(f"Sys path: {sys.path}")
import os
print(f"Current dir: {os.getcwd()}")
print(f"os env pythonpath: {os.environ["PYTHONPATH"]}")

from cardimg_add_batch import app as cardimg_add_batch_app
from conftest import CSVUPLOAD_HAPPY_CASES, CSVUPLOAD_ERROR_CASES
from moto import mock_aws

@mock_aws
@pytest.mark.parametrize("event_file,expected_status,error_substring", CSVUPLOAD_ERROR_CASES)
def test_error_handling(load_event, event_file, expected_status, testname):
    """Test that all error cases return appropriate status codes and messages."""
    event = load_event(event_file)
    response = cardimg_add_batch_app.lambda_handler(event, {})
    
    assert response['statusCode'] == expected_status
