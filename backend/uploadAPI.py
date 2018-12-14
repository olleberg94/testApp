import csv
import json
import requests
from flask import Flask, render_template
import google.cloud
from google.cloud import pubsub
from google.cloud import bigquery


def retriveapi(requestURL, dataSet, tableName):
    # CREATE API REQUEST
    queryClient = bigquery.Client()
    request = requests.get(url=requestURL, params='')
    response = request.json()
    data = json.dumps(response)
    result = json.loads(data)
    # SOME TRANSFORMATION
    '''
    for x in result:
        if (x['id'] == 2):
            x['name'] = 'Olles Bryggeri'
    '''
    header = []
    for item in result[0].keys():
        header.append(item)

    for line in result:
        for item in line:
            if (type(line[item]) is not str):
                if (type(line[item]) is not int):
                    line[item] = str(line[item])

    bigQuerySchema = []
    for item in result[0]:
        listOfItems = result[0]
        x = type(listOfItems[item])
        if (x is str):
            bigQuerySchema.append(bigquery.SchemaField(item, 'STRING'))
        if (x is int):
            bigQuerySchema.append(bigquery.SchemaField(item, 'INTEGER'))
    datasetRef = queryClient.dataset(dataSet)
    try:
        dataset = bigquery.Dataset(datasetRef)
        dataset.location = 'US'
        dataset = queryClient.create_dataset(dataset)
        print('Dataset created')
    except:
        print('Dataset Already exsists')
    tableRef = datasetRef.table(tableName)
    table = bigquery.Table(tableRef, schema=bigQuerySchema)
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

        for jsonObj in result:
            sourceFile = open('tmp.json', 'w+')
            jsonItem = json.dumps(jsonObj)
            sourceFile.write(str(jsonItem))
            sourceFile.close()
            sourceFile = open('tmp.json', 'rb')
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
        rowNumber = destination_table.num_rows
        return 'Loaded '+str(rowNumber)+' rows to database'


