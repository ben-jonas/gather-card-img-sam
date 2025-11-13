import boto3, os, pytest

from moto import mock_aws

@mock_aws
@pytest.mark.parametrize(
    "event_file, expected_status", pytest.CSVUPLOAD_PAYLOADERROR_CASES # type:ignore[reportAttributeAccessIssue]
    ) 
def test_payload_errors(load_event, event_file, expected_status):
    """Test that all error cases return appropriate status codes."""
    set_env_vars()
    from cardimg_add_batch import app as cardimg_add_batch_app
    event = load_event(event_file)
    response = cardimg_add_batch_app.lambda_handler(event, {})
    assert response['statusCode'] == expected_status

@mock_aws
@pytest.mark.parametrize(
    "event_file, expected_status", pytest.CSVUPLOAD_HAPPY_CASES # type:ignore[reportAttributeAccessIssue]
    ) 
def test_valid_csvs(load_event, event_file, expected_status):
    """Test that all success cases return appropriate status codes."""
    set_env_vars()
    from cardimg_add_batch import app as cardimg_add_batch_app
    event = load_event(event_file)
    response = cardimg_add_batch_app.lambda_handler(event, {})
    assert response['statusCode'] == expected_status

def set_env_vars():
    os.environ['APPROVED_DOMAINS_TO_CARDIMG_SELECTORS'] = '{"scryfall.com": "card", "pkmncards.com": "card-image"}'
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = sqs.create_queue(QueueName='testq')['QueueUrl']
    os.environ['CARD_IMG_FETCH_QUEUE'] = queue_url