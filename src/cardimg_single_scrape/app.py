from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, ParseResult
import boto3, json, mimetypes, os, re, requests, time

CARDIMG_BUCKET = os.environ['CARDIMG_BUCKET']
SCRAPER_APP_VERSION = os.environ['SCRAPER_APP_VERSION']
APPROVED_DOMAINS_TO_CARDIMG_SELECTORS = \
    json.loads(os.environ['APPROVED_DOMAINS_TO_CARDIMG_SELECTORS'])
CARD_PAGE_URI_COLUMN = "Card Page URI"

# Matches the img src as capture grp 1 and the query params as grp 3
# Only matches raster image types (excludes icons and vector images).
IMG_SRC_REGEX_PATTERN = r'^(.*\.(jpg|jpeg|png|gif|webp|avif|bmp|tiff|tif))(\?.*)?$'

SLEEP_TIME = .1

SCRAPE_SUCCESS_STATUS = "SUCCESS"
SCRAPE_FAILURE_STATUS = "FAILURE"

s3 = boto3.client('s3')
dynamodb = boto3.resource("dynamodb")
batchStatusTable = dynamodb.Table("CardImgBatchStatus") # type:ignore[reportAttributeAccessIssue]

def lambda_handler(event, context):
    sqs_record_bodies = [ json.loads(record['body']) for record in event['Records'] ]
    print(f"Handling event with {len(sqs_record_bodies)} csv entries.")
    print(json.dumps(event))

    # Our way of handling failed scrape requests is unsophisticated. An exception that occurs
    # partway through the batch of queued events will cause this whole lambda invocation to fail.
    # Any unprocessed queue events will be dropped, so we have to save them as failures during
    # exception processing. Therefore, we assign the statuses to all failures until we confirm
    # their success, one at a time.
    # Here we construct a two-level dictionary, wherein batch ID's (referring to user-defined
    # batches, NOT SQS batches) map to dicts of URLs to statuses. Statuses are initialized to
    # a failure status.
    statuses = {}
    for sqs_record in sqs_record_bodies:
        if not sqs_record['batchId'] in statuses:
            statuses[sqs_record['batchId']] = {}
        statuses_for_batch_id = statuses[sqs_record['batchId']]
        cardpage_uri = sqs_record['itemFromBatch']['Card Page URI']
        statuses_for_batch_id[cardpage_uri] = SCRAPE_FAILURE_STATUS

    for record_body in sqs_record_bodies:
        batch_id, item_from_batch = record_body['batchId'], record_body['itemFromBatch']
        try:
            parsed_cardpage_uri = urlparse(item_from_batch['Card Page URI'].removesuffix('/'))
            cardpage_domain = parsed_cardpage_uri.netloc.lower().removeprefix("www.")
            cardimg_selector = APPROVED_DOMAINS_TO_CARDIMG_SELECTORS[cardpage_domain]
            if already_has_s3_key_at(parsed_cardpage_uri):
                print(f"Object already exists at {get_s3_prefix_for_cardimg(parsed_cardpage_uri)}.")
            else:
                print(f"Object not found at {get_s3_prefix_for_cardimg(parsed_cardpage_uri)}. Retrieving it from {parsed_cardpage_uri.netloc}...")
                locate_and_upload_img(parsed_cardpage_uri, cardimg_selector)
            
            # save success to dynamo
            save_job_status_to_dynamo(batch_id, item_from_batch['Card Page URI'], SCRAPE_SUCCESS_STATUS)
            statuses[batch_id][item_from_batch['Card Page URI']] = SCRAPE_SUCCESS_STATUS
            
        except Exception as e:
            print(f"Exception occured: {str(e)}")
            # save all failures (and pending jobs we didn't get to) as failures in dynamo
            for pending_batch_id, statuses_for_id in statuses.items():
                for uri, status in statuses_for_id.items():
                    if not status == SCRAPE_SUCCESS_STATUS:
                        save_job_status_to_dynamo(pending_batch_id, uri, SCRAPE_FAILURE_STATUS)
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Internal server error'})
            }
    return {
        "statusCode": 200,
        'headers': {'Content-Type': 'application/json'},
        "body": json.dumps({
            "message": f"located + uploaded {len(sqs_record_bodies)} objects"
        }),
    }

def already_has_s3_key_at(parsed_cardpage_uri:ParseResult) -> bool:
    s3_response = s3.list_objects_v2(
        Bucket=CARDIMG_BUCKET,
        Prefix=get_s3_prefix_for_cardimg(parsed_cardpage_uri)
    )
    return s3_response.get('Contents') is not None

def locate_and_upload_img(parsed_cardpage_uri:ParseResult, cardimg_selector:str):
    cardimg_uri = get_cardimg_uri(parsed_cardpage_uri, cardimg_selector)

    time.sleep(SLEEP_TIME)
    resp = requests.get(cardimg_uri)
    if not resp.ok:
        raise RuntimeError("status code was " + str(resp.status_code))
    imgdata = resp.content

    prefix = get_s3_prefix_for_cardimg(parsed_cardpage_uri)
    terminal_name = "img." + urlparse(cardimg_uri).path.split('.')[-1] # "img." + file extension
    cardimg_s3key = prefix + terminal_name
    s3.put_object(
                Bucket=CARDIMG_BUCKET,
                Key=cardimg_s3key,
                Body=imgdata,
                ContentType=mimetypes.guess_file_type(terminal_name)[0],
                Metadata={
                    "scraper_app_version": SCRAPER_APP_VERSION,
                    "datetime": datetime.now().isoformat(),
                    "original_img_uri": cardimg_uri
                }
    )

def get_cardimg_uri(cardpage_uri:ParseResult, css_class:str) -> str:
    time.sleep(SLEEP_TIME)
    resp = requests.get(cardpage_uri.geturl())
    if not resp.ok:
        raise RuntimeError("status code was " + str(resp.status_code))
    resp_soup = BeautifulSoup(resp.text, 'html.parser')
    cardimg_tag = resp_soup.find_all("img", class_=css_class)[0]
    return clean_cardimg_uri(str(cardimg_tag['src']))

def clean_cardimg_uri(cardimg_uri:str) -> str:
    match = re.match(IMG_SRC_REGEX_PATTERN, cardimg_uri, re.IGNORECASE)
    if not match:
        raise ValueError(
            "Invalid image URL: '"
            + cardimg_uri
            + "' does not end with a valid image extension")
    return match.group(1)

def get_s3_prefix_for_cardimg(parsed_cardpage_uri:ParseResult) -> str:
    return f"{(parsed_cardpage_uri.netloc + parsed_cardpage_uri.path).lower()}/"

def save_job_status_to_dynamo(batch_id:str, scraped_uri:str, status:str):
    return batchStatusTable.update_item(
                Key={'batchId': batch_id},
                UpdateExpression=f'SET progressDocument.#scraped_uri = :status',
                ExpressionAttributeNames={
                    '#scraped_uri': scraped_uri
                },
                ExpressionAttributeValues={
                    ':status': status
                }
            )