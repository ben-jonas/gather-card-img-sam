import boto3, json

dynamodb = boto3.resource("dynamodb")
batchStatusTable = dynamodb.Table("CardImgBatchStatus") #type:ignore[reportAttributeAccessIssue]

def lambda_handler(event, context):
    batch_id = event['pathParameters']['batchId']
    try:
        query_result = batchStatusTable.get_item(
            Key={'batchId': batch_id}
            )
        if 'Item' not in query_result:
            raise ItemNotFoundInTableException(f"No batch found with the given id")
        batch_status_item = query_result['Item']
        progress_document = batch_status_item['progressDocument']
    except ItemNotFoundInTableException as e:
        return {
            'statusCode': 404,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error'})
        }

    return {
        "statusCode": 200,
        'headers': {'Content-Type': 'application/json'},
        "body": json.dumps({
            "batchId": batch_id,
            "progress": progress_document
        }),
    }

class ItemNotFoundInTableException(Exception):
    pass