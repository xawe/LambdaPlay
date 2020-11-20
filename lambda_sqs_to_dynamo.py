import json
import boto3
import uuid

def lambda_handler(event, context):
    # TODO implement
    for record in event['Records']:
      print ("test")
      payload=json.loads(record["body"])
      print(payload['name'])
      print(payload['age'])
      print(payload['city'])
      print(str(payload))
      dynamodb = boto3.resource('dynamodb')
      table = dynamodb.Table('data1')
      id = uuid.uuid1()
      
      table.put_item(
          Item={
           'id': str(id),
           'name': payload['name'],
           'age': payload['age'],
           'city': payload['city']
          }
          )
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
