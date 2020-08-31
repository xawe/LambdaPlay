import boto3


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    # TODO implement
    print(event)
    print('bucket >> ')
    bucket = event['Records'][0]['s3']['bucket']['name']
    print(bucket)
    print('printando key')
    key = event['Records'][0]['s3']['object']['key']
    print(key)
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    print('linha >> ')
    a = "111010101"
    print(a)
    
    print("obtendo tabela dynamo")
    table = dynamodb.Table('teste_lambda2')
    
    # table.put_item(
    #     Item={
    #         'id': 1,
    #         'name': 'Alam',
    #         'document': '21848764880'
    #     })
    i = 6
    with table.batch_writer() as batch:
        for item in obj['Body'].read().decode("utf-8").split('\n'):
            i = i + 1
            # TODO: write code...
            print(item)
            objeto = item.split(',')
            id = ''
            if(objeto is None):
                if(not objeto[0] is None):
                    id = objeto[0]
            print('printando colunas encontradas')
            if(not id is None):
                print(i+1)
                batch.put_item(Item={
                    'id':str(i),
                    'name':objeto[1],
                    'document': objeto[2]                    
                })
                print("Adicionado registro id >> " + id)
            
        
    
    
    
    
    
