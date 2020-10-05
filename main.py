import boto3

s3 = boto3.resource('s3', aws_access_key_id='AKIAIOYXEMM6O7AF6F7A', aws_secret_access_key='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')
bucket = s3.Bucket('datacont-jamespickering')
bucket.Acl().put(ACL='public-read')
body = open('MODERN ART.jpg', 'rb')
o = s3.Object('datacont-jamespickering', 'test').put(Body=body)
s3.Object('datacont-jamespickering', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
 region_name='us-east-2',
 aws_access_key_id='AKIAIOYXEMM6O7AF6F7A',
 aws_secret_access_key='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'
)

try:
 table = dyndb.create_table(
 TableName='DataTable3',
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
 }
 )
except:
 #if there is an exception, the table may already exist. if so...
 table = dyndb.Table("DataTable3")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable3')

print(table.item_count)

import csv

with open('stupidcsv.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open(item[3]+'.csv', 'rb')
        s3.Object('datacont-jamespickering', item[3]).put(Body=body )
        md = s3.Object('datacont-jamespickering', item[3]).Acl().put(ACL='public-read')

        url = "https://s3-us-east-2.amazonaws.com/datacont-jamespickering/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description' : item[4], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
 Key={'PartitionKey': 'Experiment 2','RowKey': '1'}
)
item = response['Item']
print(item)