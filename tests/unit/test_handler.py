import pytest

from cardimg_add_batch import app as cardimg_add_batch_app
from moto import mock_aws

@mock_aws
@pytest.mark.parametrize(
    "event_file, expected_status", pytest.CSVUPLOAD_PAYLOADERROR_CASES # type:ignore[reportAttributeAccessIssue]
    ) 
def test_payload_errors(load_event, event_file, expected_status):
    """Test that all error cases return appropriate status codes."""
    event = load_event(event_file)
    response = cardimg_add_batch_app.lambda_handler(event, {})
    assert response['statusCode'] == expected_status

@mock_aws
@pytest.mark.parametrize(
    "event_file, expected_status", pytest.CSVUPLOAD_HAPPY_CASES # type:ignore[reportAttributeAccessIssue]
    ) 
def test_valid_csvs(load_event, event_file, expected_status):
    """Test that all error cases return appropriate status codes."""
    event = load_event(event_file)
    response = cardimg_add_batch_app.lambda_handler(event, {})
    assert response['statusCode'] == expected_status