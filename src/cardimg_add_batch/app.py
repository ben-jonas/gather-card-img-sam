import boto3, csv, json, os
from io import StringIO
from typing import Any
from urllib.parse import urlparse

services: dict[str, Any] = {
    "sqs": None
}

def _init_aws_srvcs_once():
    if not services["sqs"]:
        services["sqs"] = boto3.client("sqs")


# We only care about the keys of the APPROVED_DOMAINS_TO_CARDIMG_SELECTORS
# variable; the values are only relevant to the 'single scrape' lambda.
APPROVED_DOMAINS = \
    list(json.loads(os.environ['APPROVED_DOMAINS_TO_CARDIMG_SELECTORS']).keys())
CARD_IMG_FETCH_QUEUE = os.environ['CARD_IMG_FETCH_QUEUE']
CARD_PAGE_URI_COLUMN = "Card Page URI"

def lambda_handler(event, context) -> dict[str, Any]:
    try:
        _init_aws_srvcs_once()
        data, user_errors = validate_event(event)
        if user_errors:
            return {
                "statusCode": 400,
                "headers": {'Content-Type': 'application/json'},
                "body": json.dumps(user_errors),
            }
        else:
            for row in data:
                send_imgrequest_to_sqs(row)
            return {
                "statusCode": 200,
                "headers": {'Content-Type': 'application/json'},
                "body": json.dumps(data),
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(type(e))})
        }

def send_imgrequest_to_sqs(csv_row: dict[str, str]) -> None:
    pass
    
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
        else:
            cardpage_uri = urlparse(cardpage_uri_text.lower())
            if not (cardpage_uri.scheme and cardpage_uri.netloc):
                errors.append("uri scheme and/or netloc missing (uri should begin with 'https://www.some-domain')")
            elif cardpage_uri.scheme != "https":
                errors.append("uri must begin with https")
            elif cardpage_uri.netloc.removeprefix('www.') not in APPROVED_DOMAINS:
                errors.append("uri not in approved domains")
    except KeyError as ke:
        errors.append("malformed row")
    return errors