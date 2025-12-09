import boto3, json

dynamodb = boto3.resource("dynamodb")
batchStatusTable = dynamodb.Table("CardImgBatchStatus") #type:ignore[reportAttributeAccessIssue]

def lambda_handler(event, context):
    batch_id = event['pathParameters']['batchId']
    query_result = batchStatusTable.get_item(
        Key={'batchId': batch_id}
        )
    batch_status_item = query_result['Item']
    progress_document = batch_status_item['progressDocument']

    return {
        "statusCode": 200,
        'headers': {'Content-Type': 'application/json'},
        "body": json.dumps({
            "batchId": batch_id,
            "progress": progress_document
        }),
    }
