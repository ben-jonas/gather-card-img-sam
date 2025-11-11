import boto3, csv, json, os
from io import StringIO
from typing import Any
from urllib.parse import urlparse

services: dict[str, Any] = {
    "sqs": None
}
# This allows mocking of boto3 in tests of lambda_handler() while retaining
# the benefits of lambda execution environment reuse out in production
def _init_aws_srvcs_once():
    if not services["sqs"]:
        services["sqs"] = boto3.client("sqs")

# If we see non-approved domains in the csv file, we want to exit early before
# passing anything to sqs.
# We only care about the keys of the APPROVED_DOMAINS_TO_CARDIMG_SELECTORS
# variable; the values are only relevant to the 'single scrape' lambda.
APPROVED_DOMAINS = \
    list(json.loads(os.environ['APPROVED_DOMAINS_TO_CARDIMG_SELECTORS']).keys())

CARD_PAGE_URI_COLUMN = "Card Page URI"

def lambda_handler(event, context) -> dict[str, Any]:
    try:
        _init_aws_srvcs_once()
        user_errors = {}
        data = []
        try:
            csv_headercheck = list(csv.reader(StringIO(event['body'])))
            if CARD_PAGE_URI_COLUMN not in csv_headercheck[0]:
                raise ValueError(f"Missing column {CARD_PAGE_URI_COLUMN}")
            csv_reader = csv.DictReader(StringIO(event['body']))
        except KeyError:
            user_errors["bodyErrors"] = ["Request body missing or inaccessible"]
        except ValueError:
            user_errors["bodyErrors"] =["CSV headers missing or malformed"]
        else:
            data = list(csv_reader)
            single_row_errors = validate_csvdata_singlerows(data)
            if single_row_errors:
                user_errors["singleRowErrors"] = single_row_errors
            # Could add cross-field validations here. Perhaps to enforce uniqueness?

        if user_errors:
            return {
                "statusCode": 400,
                "headers": {'Content-Type': 'application/json'},
                "body": json.dumps(user_errors),
            }
            
        return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
             "body": json.dumps(data),
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'error': str(type(e))})
        }
    

def validate_csvdata_singlerows(csvdata:list[dict[str,str]]) -> dict[int,list[str]]:
    errors_by_row = {}
    for (i, row) in  enumerate(csvdata):
        row_errors = []
        row_errors.extend(validate_cardpage_uri(row))
        #additional validations would go here
        if row_errors:
            # i + 2 because a text file is 1-indexed, not 0-indexed, and also the
            # first row is the column names
            errors_by_row[i+2] = row_errors
    return errors_by_row

def validate_cardpage_uri(csv_row:dict[str, str]) -> list[str]:
    errors = []
    try:
        cardpage_uri = urlparse(csv_row[CARD_PAGE_URI_COLUMN].lower())
        if not (cardpage_uri.scheme and cardpage_uri.netloc):
            errors.append("uri scheme and/or netloc missing (uri should begin with 'https://www.some-domain')")
        elif cardpage_uri.scheme != "https":
            errors.append("uri must begin with https")
        elif cardpage_uri.netloc.removeprefix('www.') not in APPROVED_DOMAINS:
            errors.append("uri not in approved domains")
    except KeyError as ke:
        errors.append("malformed row")
    return errors