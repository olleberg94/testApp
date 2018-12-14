import base64
import csv
import json
import requests
import google.cloud
from google.cloud import storage as sto
from google.cloud import pubsub
from google.cloud import bigquery


def uploadToBigQuery(bucketName, fileName, dataSet, tableName):
    queryClient = bigquery.Client()
    storageClient = sto.Client()
    bucket = storageClient.get_bucket(bucketName)
    blob = bucket.blob(fileName)
    dataString = blob.download_as_string()
    items = dataString.splitlines()
    # Create DICTIONARY WITH NECESSARY ITEMS FROM CSV FILE
    dataDict = {}
    itemsInLine = []
    listOfDicts = []
    # dataCSV = open('tmp.csv', 'w+')
    # csvwriter = csv.writer(dataCSV)
    count = 0
    formattedHeader = str(items[0])
    headers = formattedHeader[2:-1].split(',', -1)
    del items[0]
    for char in items:
        tmp = str(char)
        substringedRow = tmp[2:-1]
        if (substringedRow != '\'\''):
            words = substringedRow.split(',', -1)
            noOfWords = len(words)
            if (noOfWords > 2):
                itemsInLine.append(words)
    bigIterCount = 0
    smallIterCount = 0
    while (bigIterCount < len(itemsInLine)):
        strings = itemsInLine[bigIterCount]
        for header in headers:
            dataDict[header] = strings[smallIterCount]
            if (smallIterCount < len(strings)):
                smallIterCount += 1
        listOfDicts.append(dataDict.copy())
        bigIterCount += 1
        smallIterCount = 0
        # INSERT INTO BIGQUERYDATABASE
    # CREATE TABLE
    datasetRef = queryClient.dataset(dataSet)
    try:
        dataset = bigquery.Dataset(datasetRef)
        dataset.location = 'US'
        dataset = queryClient.create_dataset(dataset)
        print('Dataset created')
    except:
        print('Dataset Already exsists')
    tableRef = datasetRef.table(tableName)
    bigQuerySchema = []
    for header in headers:
        bigQuerySchema.append(bigquery.SchemaField(header, 'STRING'))
    table = bigquery.Table(tableRef, schema=bigQuerySchema)
    print(table)
    try:
        table = queryClient.create_table(table)
        assert table.table_id == tableName
        print('Creating Table')
    except:
        print('Table already exists')
    finally:
        # INSERT INTO TABLE
        jobConfig = bigquery.LoadJobConfig()
        jobConfig.schema = bigQuerySchema
        jobConfig.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

        for jsonObj in listOfDicts:
            sourceFile = open('/tmp/tmp.json', 'w+')
            jsonItem = json.dumps(jsonObj)
            sourceFile.write(str(jsonItem))
            sourceFile.close()
            sourceFile = open('/tmp/tmp.json', 'rb')
            load_job = queryClient.load_table_from_file(
                sourceFile,
                tableRef,
                location='US',
                job_config=jobConfig)
            print('Starting job {}'.format(load_job.job_id))
            load_job.result()
            print('Row Finished.')
            destination_table = queryClient.get_table(datasetRef.table(tableName))
            print('Added: ' + str(jsonObj))
            print('Loaded {} rows.'.format(destination_table.num_rows))

        print('Job finished.')
        destination_table = queryClient.get_table(datasetRef.table(tableName))
        print('Loaded {} rows.'.format(destination_table.num_rows))
        return 'Table Loaded'

#ADD SUBSCRIPER PULL FOR TEXTUPLOAD
def bigQueryImport(data, context):
    attributes = data['attributes']
    if ('bucketName' in attributes.keys()):
        bucketName = attributes['bucketName']
        fileName = attributes['fileName']
        dataSet = attributes['dataSet']
        table = attributes['table']
        print(bucketName + fileName + dataSet + table)
        uploadToBigQuery(bucketName, fileName, dataSet, table)
        publisher = pubsub.PublisherClient()
        publisher.publish('projects/balmy-component-220214/topics/databaseRetrieve', b'response', project=bucketName, tableName=table, dataset=dataSet)
        return 'Table Loaded'
    else:
        return 'Invalid Table input, wrong formatting'