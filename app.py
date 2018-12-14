from flask import Flask, request, render_template
from backend import uploadAPI
from backend import returnTable



app = Flask(__name__)

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/loadapi', methods=['POST', 'GET'])
def loadapi():
    import json
    requestURL = request.form['requestURL']
    dataSet = request.form['dataSet']
    tableName = request.form['tableName']
    project = request.form['project']
    print('hello')
    if request.method == 'POST':
        print('entered')
        uploadAPI.retriveapi(requestURL, dataSet, tableName)
        jsonObj = returnTable.loadInsertedAPI(project, dataSet, tableName)
        dictionary = json.loads(jsonObj)
        colNames = []
        for item in dictionary[0]:
            colNames.append(item)
        return render_template("loadapi.html", records=dictionary, colnames=colNames)
    else:
        return 'Method Error'

if __name__ == '__main__':
    app.run(debug=True)
