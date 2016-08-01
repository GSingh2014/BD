'''
Created on Jun 27, 2016

@author: gopasing
'''

from flask import Flask,jsonify
from flask import request
from flask_restful import Api,Resource

import urllib.request

import pandas as pd

apptux = Flask(__name__)
api = Api(app=apptux)

@apptux.route('/')
def index():
    args = request.args
    print (args) # For debugging
    no1 = args['key1']
    no2 = args['key2']
    searchUrl = 'http://mcp-bld-lnx-330:8986/solr/cisco.cdetsdetails/select?q=*%3A*&fl=*&wt=json&indent=true'
    try:
        with urllib.request.urlopen(searchUrl) as response:
            res = response.read()
            df = pd.read_json(res)
            df.
            wrkdf = df['response.docs']
            wrkdf.columns
            
        
    except urllib.error.URLError as e:
        print(e.reason)    
    return jsonify(res)

if __name__ == '__main__':
    apptux.run(debug=True)
    