import boto3, time, os, pytest

from moto import mock_aws
from moto.core.models import patch_client, patch_resource

@mock_aws
@pytest.mark.parametrize(
        "event_file, expected_status",
        pytest.CSVUPLOAD_PAYLOADERROR_CASES # type:ignore[reportAttributeAccessIssue]
    ) 
def test_payload_errors(load_event, event_file, expected_status):
    """Test that all error cases return appropriate status codes."""
    set_env_vars_and_aws_resources()
    from cardimg_add_batch import app as cardimg_add_batch_app
    patch_client(cardimg_add_batch_app.sqs)
    patch_resource(cardimg_add_batch_app.dynamodb)
    event = load_event(event_file)
    response = cardimg_add_batch_app.lambda_handler(event, {})
    assert response['statusCode'] == expected_status

@mock_aws
@pytest.mark.parametrize(
        "event_file, expected_status",
        pytest.CSVUPLOAD_HAPPY_CASES # type:ignore[reportAttributeAccessIssue]
    )
def test_valid_csvs(load_event, event_file, expected_status):
    """Test that all success cases return appropriate status codes."""
    set_env_vars_and_aws_resources()
    from cardimg_add_batch import app as cardimg_add_batch_app
    event = load_event(event_file)
    response = cardimg_add_batch_app.lambda_handler(event, {})
    assert response['statusCode'] == expected_status

def set_env_vars_and_aws_resources():
    os.environ['APPROVED_DOMAINS_TO_CARDIMG_SELECTORS'] = '{"scryfall.com": "card", "pkmncards.com": "card-image"}'
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = sqs.create_queue(QueueName='testq')['QueueUrl']
    os.environ['CARD_IMG_FETCH_QUEUE'] = queue_url
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    dynamodb.create_table( # type:ignore[reportAttributeAccessIssue]
        TableName='CardImgBatchStatus',
        KeySchema=[
            { 'AttributeName': 'batchId', 'KeyType': 'HASH' }
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'batchId', 'AttributeType': 'S' }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    table_is_active = False
    for _ in range(10):
        table_description = dynamodb.describe_table(TableName='CardImgBatchStatus')
        table_is_active = table_description['Table']['TableStatus'] == 'ACTIVE'
        if table_is_active:
            break
        time.sleep(.05)
    if not table_is_active:
        raise Exception("CardImgBatchStatus table never activated during setup")
    
