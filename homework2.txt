import boto3
import csv

s3 = boto3.resource('s3',
 aws_access_key_id='A',
 aws_secret_access_key='B'
)
try:
    s3.create_bucket(Bucket='datacont-tristin-butz', CreateBucketConfiguration={
     'LocationConstraint': 'us-east-2'})
except:
    print("this may already exist")

bucket = s3.Bucket("datacont-tristin-butz")
bucket.Acl().put(ACL='public-read')
body = open('experiments.csv', 'rb')
o = s3.Object('datacont-tristin-butz', 'test').put(Body=body)
s3.Object('datacont-tristin-butz', 'test').Acl().put(ACL='public-read')
dyndb = boto3.resource('dynamodb',
 region_name='us-east-2',
 aws_access_key_id='AKIA2NLU2T7NEIKJRH76',
 aws_secret_access_key='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'
)
try:
    table = dyndb.create_table(
    TableName='Datatable',
    KeySchema=[
    {
    'AttributeName': 'PartitionKey',
    'KeyType': 'HASH'
    },
    {
    'AttributeName': 'RowKey',
    'KeyType': 'RANGE'
    }
    ],
    AttributeDefinitions=[
    {
    'AttributeName': 'PartitionKey',
    'AttributeType': 'S'
    },
    {
    'AttributeName': 'RowKey',
    'AttributeType': 'S'
    },
    ],
    ProvisionedThroughput={
    'ReadCapacityUnits': 5,
    'WriteCapacityUnits': 5
    })
except:
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("Datatable")

table.meta.client.get_waiter('table_exists').wait(TableName='Datatable')

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        if(item[0] != "partition"):
            body = open(item[4], 'rb')
            s3.Object('datacont-tristin-butz', item[3]).put(Body=body)
        md = s3.Object('datacont-tristin-butz', item[3]).Acl().put(ACL='public-read')
        url = " https://s3-us-east-2.amazonaws.com/datacont-tristin-butz/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
        'description' : item[4], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
 Key={
 'PartitionKey': 'experiment1',
 'RowKey': 'data1'
 }
)
item = response['Item']
print(item)


