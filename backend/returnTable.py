import requests
import json
import google.cloud
from google.cloud import bigquery
from google.cloud import storage

#storageclient = storage.client()
queryclient = bigquery.Client()

#RETURN DATABASE INSERT
def loadInsertedAPI(project, dataset, tableName):
    queryJob = ('SELECT * FROM `'+project+'.'+dataset+'.'+tableName+'` LIMIT 10')
    resultDf = queryclient.query(queryJob).to_dataframe()
    resultSet = resultDf.to_dict('records')
    jsonObj = json.dumps(resultSet)
    return jsonObj



#ADD RUN METHON AND SUB PULL FROM DATABASERETRIEVE
