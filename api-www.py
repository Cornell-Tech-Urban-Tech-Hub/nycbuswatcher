import os
from datetime import date, datetime
from dateutil import parser


from flask import Flask, render_template, request, jsonify, abort, send_from_directory
from flask_cors import CORS
from flask_restful import Resource, Api
from marshmallow import Schema, fields

from sqlalchemy import create_engine

import Database as db

from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv("MAPBOX_API_KEY")
api_url_stem="/api/v1/nyc/livemap"

from config import config

#-----------------------------------------------------------------------------------------
# sources
#
# api approach adapted from https://www.codementor.io/@sagaragarwal94/building-a-basic-restful-api-in-python-58k02xsiq
# query parameter handling after https://stackoverflow.com/questions/30779584/flask-restful-passing-parameters-to-get-request
#-----------------------------------------------------------------------------------------


#--------------- INITIALIZATION ---------------

db_connect = create_engine(db.get_db_url(config.config['dbuser'], config.config['dbpassword'], config.config[
    'dbhost'], config.config['dbport'], config.config['dbname']))
# to 'localhost' for debugging?
app = Flask(__name__,template_folder='./api-www/templates',static_url_path='/static',static_folder="api-www/static/")
api = Api(app)
CORS(app)


#--------------- HELPER FUNCTIONS ---------------

def unpack_query_results(query):
    return [dict(zip(tuple(query.keys()), i)) for i in query.cursor]

def sparse_unpack_for_livemap(query):
    unpacked = [dict(zip(tuple(query.keys()), i)) for i in query.cursor]
    sparse_results = []
    for row in unpacked:
        sparse_results.append('something')
    return unpacked

def query_builder(parameters):
    query_suffix = ''
    for field, value in parameters.items():
        if field == 'output':
            continue


        # todo query optimization--convert this to get the date part of the start date and query service_date instead
        elif field == 'start':
            query_suffix = query_suffix + '{} >= "{}" AND '\
                .format('timestamp',parser.isoparse(value.replace(" ", "+", 1)))
                # replace is a hack but gets the job done because + was stripped from url replaced by space
            continue

        # todo query optimization--convert this to get the date part of the start date and query service_date instead
        elif field == 'end':
            query_suffix = query_suffix + '{} < "{}" AND '\
                .format('timestamp', parser.isoparse(value.replace(" ", "+", 1)))
            continue
        elif field == 'route_short':
            query_suffix = query_suffix + '{} = "{}" AND '.format('route_short', value)
            continue
        else:
            query_suffix = query_suffix + '{} = "{}" AND '.format(field,value)
    query_suffix=query_suffix[:-4] # strip tailing ' AND'
    return query_suffix

def results_to_FeatureCollection(results):
    geojson = {'type': 'FeatureCollection', 'features': []}
    for row in results['observations']:
        feature = {'type': 'Feature',
                   'properties': {},
                   'geometry': {'type': 'Point',
                                'coordinates': []}}
        feature['geometry']['coordinates'] = [row['lon'], row['lat']]
        for k, v in row.items():
            if isinstance(v, (datetime, date)):
                v = v.isoformat()
            feature['properties'][k] = v
        print (feature)
        geojson['features'].append(feature)
    return geojson

def results_to_KeplerTable(query):
    results = query['observations']
    fields = [{"name":x} for x in dict.keys(results[0])]

    # make the fields list of dicts
    field_list =[]
    for f in fields:
        fmt='TBD'
        typ=type(f)
        # field_list.append("{name: '{}', format '{}', type:'{}'},".format(f,fmt,typ))
        # field_list.append("{name: '{}'},".format(f))
        field_list.append("{'TBD':'TBD',")
    # make the rows list of lists
    rows = []
    for r in results:
        (a, row)= zip(*r.items())
        rows.append(r)
    kepler_bundle = {"fields": fields, "rows": rows }
    return kepler_bundle


#--------------- API ---------------


#--- ALL UNIQUE ROUTES IN HISTORY (JSON)---#
class KnownRoutes(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("SELECT DISTINCT route_short FROM buses")
        results = {'routes': [i[0] for i in query.cursor.fetchall()]}
        return jsonify(results)

#--- ALL BUSES IN LAST 60 SECONDS FOR LIVE MAP (GEOJSON) ---#
class LiveMap(Resource):
    def get(self):
        conn = db_connect.connect()
        query = conn.execute("SELECT * FROM buses WHERE timestamp >= NOW() - INTERVAL 60 SECOND;")
        results = {'observations': unpack_query_results(query)}
        geojson = results_to_FeatureCollection(results)
        return geojson
class LiveMap2(Resource):
    def get(self):
        import geojson
        with open('./api-www/static/lastknownpositions.geojson', 'r') as infile:
            return geojson.load(infile)


#--- ALL OBSERVATIONS FOR A SINGLE UNIQUE TRIP (GEOJSON or KEPLER TABLE) ---#

# /api/v1/nyc/trips?service_date=2020-08-11
class TripQuerySchema(Schema):
    service_date = fields.Str(required=True)
    trip_id = fields.Str(required=True)
    output = fields.Str(required=True)

# trip_schema = TripQuerySchema()
#
#
# class TripAPI(Resource):
#     def get(self):
#         errors = trip_schema.validate(request.args)
#         if errors:
#             abort(400, str(errors))
#         conn = db_connect.connect()
#         query_suffix = query_builder(request.args)
#         query = conn.execute("SELECT * FROM buses WHERE {}".format(query_suffix ))
#         results = {'observations': unpack_query_results(query)}
#         if request.args['output'] == 'geojson':
#             return results_to_FeatureCollection(results)
#         elif request.args['output'] == 'kepler':
#             return jsonify(results_to_KeplerTable(results))
#

#--- ALL OBSERVATIONS FOR A WHOLE SYSTEM FOR A TIME PERIOD (GEOJSON or KEPLER TABLE) ---#

class SystemQuerySchema(Schema):
    route_short = fields.Str(required=True)
    start = fields.Str(required=False)  # in ISO 8601 e.g. 2020-08-11T14:42:00+00:00
    end = fields.Str(required=False)  # in ISO 8601 e.g. 2020-08-11T15:12:00+00:00
    output = fields.Str(required=True)

system_schema = SystemQuerySchema()

class SystemAPI(Resource):
    def get(self):
        errors = system_schema.validate(request.args)
        if errors:
            abort(400, str(errors))
        conn = db_connect.connect()
        query_prefix = "SELECT * FROM buses WHERE {}"
        query_suffix = query_builder(request.args)
        query_compound = query_prefix.format(query_suffix )
        print (query_compound)
        query = conn.execute(query_compound)
        results = {'observations': unpack_query_results(query)}
        # print (results)
        if request.args['output'] == 'geojson':
            return results_to_FeatureCollection(results)
        elif request.args['output'] == 'kepler':
            return results_to_KeplerTable(results)


#--- URLS ---#
api.add_resource(KnownRoutes, '/api/v1/nyc/knownroutes')
api.add_resource(LiveMap, '/api/v1/nyc/livemap')
api.add_resource(LiveMap2, '/api/v1/nyc/livemap2')
# api.add_resource(TripAPI, '/api/v1/nyc/trips', endpoint='trips')


#--------VIP ENDPOINT
# gives out all positions on a given 'route_short' during the specific interval in ISO 8601 format
# from 'start'
# to 'end'
# output=json for now
api.add_resource(SystemAPI, '/api/v1/nyc/buses', endpoint='buses')
# /api/v1/nyc/buses?output=geojson&route_short=Bx4&start=2021-03-28T00:00:00+00:00&end=2021-04-30T00:00:00+00:00



#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/map')
def map():
    return render_template('map.html')

@app.route('/occupancy')
def occupancy():
    return render_template('occupancy.html')

@app.route('/faq')
def faq():
    return render_template('faq.html')

# is the browser still caching this — wrap this in an http header to expire it?
@app.route('/api/v1/nyc/lastknownpositions')
def lkp():
    print (app.static_folder)
    return send_from_directory(app.static_folder,'lastknownpositions.geojson')

if __name__ == '__main__':
    app.run(debug=True)
