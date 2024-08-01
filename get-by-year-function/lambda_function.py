import boto3
import json
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

table = dynamodb.Table("movies")


# Custom encoder to convert some types.
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        elif isinstance(obj, set):
            return list(obj)
        return super(CustomEncoder, self).default(obj)


# lambda function handler method
def lambda_handler(event, context):
    query_params = event.get("queryStringParameters", {})
    year = int(query_params.get("year", 0))

    if not year:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Year parameter is missing or invalid"}),
        }

    items = []
    response = table.query(
        IndexName="year-index", KeyConditionExpression=Key("year").eq(year)
    )

    while "LastEvaluatedKey" in response:
        items.extend(response["Items"])
        response = table.query(
            IndexName="year-index",
            KeyConditionExpression=Key("year").eq(year),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )

    items.extend(response["Items"])

    return {"statusCode": 200, "body": json.dumps(items, cls=CustomEncoder)}
