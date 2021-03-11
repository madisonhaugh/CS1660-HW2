##### PART 1 #####
#Import Amazon Python Boto3 SDK
import boto3

#Access resource using a pair of access and secret keys
s3 = boto3.resource('s3',
    aws_access_key_id='AKIA3BS4M7IYPPUDJ24G', aws_secret_access_key='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'
)

#Create a new bucket to upload blobs to if it doesn't already exist
try:
    s3.create_bucket(Bucket='mrh124-hw2-bucket', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    
    #Make all blobs in the bucket publicly readable
    bucket = s3.Bucket('mrh124-hw2-bucket')
    bucket.Acl().put(ACL='public-read')
except:
    pass

#Upload a file, 'exp1.csv' into the newly created bucket
s3.Object('mrh124-hw2-bucket', 'test').put(Body=open('mydata/datafiles/exp1.csv', 'rb'))


##### PART 2 #####
#Create a DynamoDB table to store metadata and references to S3 objects
dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id='AKIA3BS4M7IYPPUDJ24G', aws_secret_access_key='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'
)

#Define table for the first time
try:
    table = dyndb.create_table (
        TableName = 'DataTable',
        KeySchema = [
            { 'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
            { 'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions = [
            { 'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
            { 'AttributeName': 'RowKey', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    
except: #If table has been previously defined, use the following code:
    table = dyndb.Table("DataTable")
    

#Wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print("Table item count: " + str(table.item_count) + "\n")

    
##### PART 3 #####
#Import CSV file
import csv
#Read the metadata from a CSV file
print("Table contents: ")
urlbase = "https://s3-us-west-2.amazonaws.com/mrh124-hw2-bucket/"
with open('mydata/experiments.csv', 'r', encoding='utf-8-sig') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        #Print the item
        print(item)
        
        #Move the data objects into the blob store
        body = open('mydata/datafiles/' + item[3], 'rb')
        s3.Object('mrh124-hw2-bucket', item[3]).put(Body=body)
        
        #Set data file URL to be publicly readable
        md = s3.Object('mrh124-hw2-bucket', item[3]).Acl().put(ACL='public-read')
        url = urlbase + item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[2], 'url': url}
        
        #Enter the metadata row into the table if it does not already exist
        try:
            table.put_item(Item = metadata_item)
        except:
            pass
        

##### PART 4 #####
#Search for an item in the table
response = table.get_item(
    Key = { 'PartitionKey': 'data2', 'RowKey': 'experiment2'}
)

#Print retrieved item
item = response['Item']
print("\nQuery results for [data2][experiment2]: " + str(item))