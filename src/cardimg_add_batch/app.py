import boto3, csv, json, os, uuid
from datetime import datetime, timedelta, UTC
from io import StringIO
from typing import Any
from urllib.parse import urlparse
from validators.url import url as validators_url

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")


# We only care about the keys of the APPROVED_DOMAINS_TO_CARDIMG_SELECTORS
# variable; the values are only relevant to the 'single scrape' lambda.
APPROVED_DOMAINS = \
    list(json.loads(os.environ['APPROVED_DOMAINS_TO_CARDIMG_SELECTORS']).keys())
CARD_IMG_FETCH_QUEUE = os.environ['CARD_IMG_FETCH_QUEUE']
CARD_PAGE_URI_COLUMN = "Card Page URI"

def lambda_handler(event, context) -> dict[str, Any]:
    try:
        data, user_errors = validate_event(event)
        if user_errors:
            return {
                "statusCode": 400,
                "headers": {'Content-Type': 'application/json'},
                "body": json.dumps(user_errors),
            }
        else:
            new_batch_id = str(uuid.uuid4())
            create_dynamo_record(data, new_batch_id)
            responses = send_csvrows_to_sqs(data, new_batch_id)
            return {
                "statusCode": 202,
                "headers": {'Content-Type': 'application/json'},
                "body": json.dumps({"batchId": new_batch_id}),
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(type(e))})
        }
    

def validate_event(event) -> tuple[list[dict[str, str]], dict[str, Any]]:
    data = []
    user_errors = {}
    try:
        csv_headercheck = csv.reader(StringIO(event['body']))
        if CARD_PAGE_URI_COLUMN not in next(csv_headercheck):
            raise ValueError(f"Missing column {CARD_PAGE_URI_COLUMN}")
        csv_reader = csv.DictReader(StringIO(event['body']))
    except (KeyError, StopIteration):
        user_errors["bodyErrors"] = "Request body missing or inaccessible"
    except ValueError:
        user_errors["bodyErrors"] ="CSV headers missing or malformed"
    else:
        data = list(csv_reader)
        single_row_errors = _validate_csvdata_singlerows(data)
        if single_row_errors:
            user_errors["singleRowErrors"] = single_row_errors
        # Could add cross-field validations here. Perhaps enforce uniqueness?

    return data, user_errors


def _validate_csvdata_singlerows(csvdata:list[dict[str,str]]) -> dict[int,list[str]]:
    errors_by_row = {}
    for (i, row) in  enumerate(csvdata):
        row_errors = []
        row_errors.extend(_validate_cardpage_uri(row))
        #additional validations would go here
        if row_errors:
            # i + 2 because a text file is 1-indexed, not 0-indexed, and also the
            # first row is the column names
            errors_by_row[i+2] = row_errors
    return errors_by_row

def _validate_cardpage_uri(csv_row:dict[str, str]) -> list[str]:
    errors = []
    try:
        cardpage_uri_text = csv_row[CARD_PAGE_URI_COLUMN]
        if not cardpage_uri_text:
            errors.append("uri missing")
        # urllib will allow a lot of arbitrary input, so this next check is to avoid js or html injections
        elif not validators_url(cardpage_uri_text):
            errors.append("uri not valid (make sure it starts with 'https://' and points to a real webpage)")
        else:
            cardpage_uri = urlparse(cardpage_uri_text.lower())
            if cardpage_uri.scheme != "https":
                errors.append("uri must begin with 'https://'")
            elif cardpage_uri.netloc.removeprefix('www.') not in APPROVED_DOMAINS:
                errors.append("uri not in approved domains")
                
    except KeyError as ke:
        errors.append("malformed row")
    return errors


def send_csvrows_to_sqs(csv_data:list[dict[str, str]], batch_id:str) -> list[dict[str,str]]:
    responses = []
    for row in csv_data:
        sqs_message_body = { "batchId": batch_id, "itemFromBatch": row}
        sqs_response = sqs.send_message(
            QueueUrl=CARD_IMG_FETCH_QUEUE,
            MessageBody=json.dumps(sqs_message_body)
        )
        responses.append(sqs_response)
    return responses


def create_dynamo_record(csv_data:list[dict[str, str]], batch_id:str):
    progress_document = {}
    for row in csv_data:
        progress_document[row[CARD_PAGE_URI_COLUMN]] = "PENDING"
    batchStatusTable = dynamodb.Table("CardImgBatchStatus") # type:ignore[reportAttributeAccessIssue]
    batchStatusTable.put_item(Item={
        "batchId": batch_id,
        "progressDocument": progress_document,
        "expiresAt": int((datetime.now(UTC) + timedelta(days=30)).timestamp())
        }
    )