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

s3 = boto3.client('s3')

def lambda_handler(event, context):
    sqs_records = event['Records']
    print(f"Handling event with {len(sqs_records)} csv entries.")
    for (i, record) in enumerate(sqs_records):
        try:
            record_body = json.loads(record['body'])
            # batch_id = record_body['batchId']
            parsed_cardpage_uri = urlparse(record_body['Card Page URI'].removesuffix('/'))
            if not (parsed_cardpage_uri.scheme == 'https'):
                raise ValueError("Cardpage link must start with https://")
            cardpage_domain = parsed_cardpage_uri.netloc.lower().removeprefix("www.")
            if cardpage_domain not in APPROVED_DOMAINS_TO_CARDIMG_SELECTORS.keys():
                raise ValueError("Cardpage domain " + cardpage_domain + 
                                 " not in approved domains")
            cardimg_selector = APPROVED_DOMAINS_TO_CARDIMG_SELECTORS[cardpage_domain]
            if already_has_key_at(parsed_cardpage_uri):
                print(f"Object already exists at {get_s3_prefix_for_cardimg(parsed_cardpage_uri)}.")
            else:
                print(f"Object not found at {get_s3_prefix_for_cardimg(parsed_cardpage_uri)}. Retrieving it from {parsed_cardpage_uri.netloc}...")
                locate_and_upload_img(parsed_cardpage_uri, cardimg_selector)
            
        except Exception as e:
            print(f"Exception occured: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Internal server error'})
            }
    return {
        "statusCode": 200,
        'headers': {'Content-Type': 'application/json'},
        "body": json.dumps({
            "message": f"located + uploaded {len(sqs_records)} objects"
        }),
    }

def already_has_key_at(parsed_cardpage_uri:ParseResult) -> bool:
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