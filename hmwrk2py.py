import boto3
s3 = boto3.resource('s3',
    aws_access_key_id='',
    aws_secret_access_key='')

try:
    s3.create_bucket(Bucket='1660hmwrk2', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("This may already exist")

bucket = s3.Bucket("1660hmwrk2")
bucket.Acl().put(ACL="public-read")

body = open(".\experiments.csv",'rb')

o = s3.Object("1660hmwrk2",'experiments').put(Body = body)
s3.Object("1660hmwrk2",'experiments').Acl().put(ACL='public-read')

# o1 = s3.object("1660hmwrk2",'exp1').put(Body = open(".\exp1.csv",'rb'))
# s3.Object("1660hmwrk2",'exp1').Acl().put(ACL='public-read')

# o2 = s3.object("1660hmwrk2",'exp2').put(Body = open(".\exp2.csv",'rb'))
# s3.Object("1660hmwrk2",'exp2').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',region_name='us-west-2',
aws_access_key_id='',
aws_secret_access_key='')

try:
    table = dyndb.create_table(
        TableName = "DataTable",
        KeySchema = [
            {'AttributeName': "PartitionKey", "KeyType": "HASH"},
            {"AttributeName": "RowKey", "KeyType": "RANGE"}
        ],
        AttributeDefinitions=[
            {"AttributeName": "PartitionKey","AttributeType": "S"},
            {"AttributeName": "RowKey", "AttributeType": "S"}
        ],
        ProvisionedThroughput=
        {'ReadCapacityUnits': 5,'WriteCapacityUnits': 5}
    )
except:
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter("table_exists").wait(TableName="DataTable")
print(table.item_count)

import csv

with open("C:\\Users\\kenan\\Documents\\University Of Pittsburgh\\Computer Science\\1660 Intro to Cloud\\homework_2\\experiments.csv","rt") as csvfile:
    csvf = csv.reader(csvfile,delimiter=',',quotechar='|')
    firstLine = True
    for item in csvf:
        if firstLine:
            firstLine = False
            continue;
        print(item[4])
        body = open("C:\\Users\\kenan\\Documents\\University Of Pittsburgh\\Computer Science\\1660 Intro to Cloud\\homework_2\\datafiles\\"+item[4],'rb')
        item[4] = item[4].split(".")[0]
        s3.Object("1660hmwrk2",item[4]).put(Body=body)
        md = s3.Object("1660hmwrk2",item[4]).Acl().put(ACL='public-read')

        url = " https://s3-us-west-2.amazonaws.com/1660hmwrk2/"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],'description' : item[3], 'date' : item[2], 'url':url} 

        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")
        
response = table.get_item(
    Key ={
        "PartitionKey": 'experiment2',
        'RowKey': 'data2'
    }
)
item = response['Item']
print(item)


