#boto3 é a lib para comunicação com os recursos amazon.
import boto3

#definindo o recurso S3
s3 = boto3.client('s3')
#definindo recurso DynamoDb
dynamodb = boto3.resource('dynamodb')

#handler padrão (main()) da função, recebendo detalhes da execução 
#através do event e context
def lambda_handler(event, context):        
    #pegando informação do bucket e key S3 que disparou a função recebido no envento
    bucket = event['Records'][0]['s3']['bucket']['name']            
    key = event['Records'][0]['s3']['object']['key']    
    #acessando o objeto S3 com as informações recebidas via event    
    obj = s3.get_object(Bucket=bucket, Key=key)    
    #acessando a tabela do dynamoDB pré criada    
    table = dynamodb.Table('teste_lambda2')

    i = 0
    #batch_writer permite fazer um batch de "inserts" no dynamo
    with table.batch_writer() as batch:
        for item in obj['Body'].read().decode("utf-8").split('\n'):
            i = i + 1                                                
            #put_item é basicamente o "insert"
            #Item é um objeto a ser inserido.
            #Em teoria, qualquer informação pode ser inserida desde que existam
            #os campos definidos como key/index            
            batch.put_item(Item={
                'id':str(i),
                'name':objeto[1],
                'document': objeto[2]                    
            })            