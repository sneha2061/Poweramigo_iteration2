import os
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('TABLE_NAME', 'SmartSensorData')
ALLOWED_ORIGIN = os.environ.get('ALLOWED_ORIGIN', '*')  # need to be set to the website frontend domain
table = dynamodb.Table(TABLE_NAME)

def _decimal_to_native(obj):
    if isinstance(obj, Decimal):
        # convert Decimal to float; change to str() if you need exact precision
        return float(obj)
    if isinstance(obj, list):
        return [_decimal_to_native(i) for i in obj]
    if isinstance(obj, dict):
return {k: _decimal_to_native(v) for k, v in obj.items()}
    return obj

def lambda_handler(event, context):
    params = (event.get('queryStringParameters') or {}) or {}
    sensor_id = params.get('id')
    limit = int(params.get('limit') or 100)
    start_ts = params.get('start_ts')
    end_ts = params.get('end_ts')

    try:
        if sensor_id:
            # If start_ts and end_ts provided, use BETWEEN; otherwise query all items for sensor_id
            if start_ts and end_ts:
                response = table.query(
                    KeyConditionExpression=Key('ID').eq(sensor_id) & Key('timestamp').between(int(start_ts), int(end_ts)),
                    Limit=limit,
                    ScanIndexForward=False
                )
            else:
                response = table.query(
                    KeyConditionExpression=Key('ID').eq(sensor_id),
                    Limit=limit,
                    ScanIndexForward=False
                )
        else:
            # No sensor id => fallback to scan (NOT recommended for very large tables)
            response = table.scan(Limit=limit)

        items = response.get('Items', [])
        items = _decimal_to_native(items)

        body = {
            "items": items,
            "count": len(items),
            "lastEvaluatedKey": response.get('LastEvaluatedKey')  # for pagination
        }

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
                "Access-Control-Allow-Headers": "Content-Type",
                "Content-Type": "application/json"
            },
            "body": json.dumps(body)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
                "Content-Type": "application/json"
            },
            "body": json.dumps({"message": "error querying DynamoDB", "error": str(e)})
        }
