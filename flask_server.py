# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import logging
from neo4jclient import Neo4jClient
import pandas as pd
 
# Flask constructor takes the name of
# current module (__name__) as argument.
app = Flask(__name__)

LOGGING_LEVEL = {
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'DEBUG': logging.DEBUG
}

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def create_app():

    app = Flask(__name__)
    CORS(app, origins='*')

    # app.run(debug=True, host='localhost', port='5001')
    with app.app_context():
        
        neo4j_client = Neo4jClient.getInstance()

        neo4j_client.init(uri="bolt://192.168.1.205:7687", user="neo4j", pwd="redefinit#2021")

        @app.route('/rest/version', methods=['GET'])
        def getVersion():
            return jsonify({"version": "1.1"})

        @app.route('/rest/query', methods=['GET'])
        def query():
            logging.debug(f"query ")
            query_res = neo4j_client.query('MATCH (n) RETURN COUNT(n) AS count')
            return jsonify(query_res)


        @app.route('/rest/query1', methods=['GET'])
        def query1():
            logging.debug(f"query1 ")
            query = "MATCH (s:SHOPS)-[SERVICESWITH]-(t:ServiceTags) return s.name as shops, s.embedding as embedding,t.name as servicetag"
            df = pd.DataFrame([dict(_) for _ in neo4j_client.query(query)])
            query_res = df.head()
            logging.debug(df.to_json())
            return df.to_json()

        @app.route('/rest/districts', methods=['GET'])
        def getDistricts():
            logging.debug("Get Districts")
            query_res = neo4j_client.query('MATCH (n:DISTRICTS) RETURN (n)')
            logging.debug(type(query_res))
            result = []
            for item in query_res:
                logging.debug(f"item is {item.data()}")
                result.append(item.data()['n'])
            return jsonify(result)

        @app.route('/rest/shops', methods=['GET'])
        def getShops():
            logging.debug("Get Shops")
            district = request.args.get('district', '')
            # query_res = neo4j_client.query('MATCH (n:SHOPS) RETURN (n)')
            query_res = neo4j_client.query('MATCH (n:SHOPS)-[:LOCATEDIN]-(d:DISTRICTS{name:"' + district + '"}) return(n) ')
            logging.debug(type(query_res))
            result = []
            for item in query_res:
                logging.debug(f"item is {item.data()}")
                result.append(item.data()['n'])
            return jsonify(result)

        @app.route('/rest/shopdetails', methods=['GET'])
        def getShopdetails():
            logging.debug("Get Shopdetails")
            district = request.args.get('district', '')
            shop = request.args.get('shop', '')
            logging.debug(f"District: {district} Shop: {shop}")
            result = []
            query_stmt = 'Match (s:SHOPS{name:"' + shop + '"})-[:SERVICESWITH]-(se:ServiceTags)-[:UsesLicenses]-(so:ServiceTags_oemnames)-[:ContainsNames]->(oe:OEMNames) return so.lastused,oe.name'

            query_res = neo4j_client.query(query_stmt)
            logging.debug(type(query_res))
            for i,item in enumerate(query_res):
                logging.debug(i)
                logging.debug(f"item is {item.data()}")
                #result.append(item.data())
                data1 = item.data()
                result.append({"name": data1.get('oe.name'), "date": data1.get('so.lastused')})
            return jsonify(result)

    return app

# main driver function
if __name__ == '__main__':
 
    # run() method of Flask class runs the application
    # on the local development server.
    app = create_app()
    app.run(debug=True, host='192.168.1.205', port='5002', threaded=True)
