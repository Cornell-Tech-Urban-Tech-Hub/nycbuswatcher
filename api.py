import os
from datetime import date, datetime, timedelta
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

def iso_to_datestr(timestamp):
    return str(timestamp).split('T',maxsplit=1)[0]

def add_1hr_to_iso(interval_start):
    one_hr = timedelta(hours=1)
    interval_end = parser.isoparse(interval_start.replace(" ", "+", 1)) + one_hr
    return interval_end.isoformat()

def query_builder(parameters):
    query_suffix = ''
    for field, value in parameters.items():
        if field == 'output':
            continue
        elif field == 'start':
            query_suffix = query_suffix + '{} >= "{}" AND {} >= "{}" AND '\
                .format('service_date',
                        iso_to_datestr(value),
                        'timestamp',
                        parser.isoparse(value.replace(" ", "+", 1))
                        )
            interval_start = value
            continue

        elif field == 'end':

            # if request is for more than one hour, truncate it to 1 hour
            interval_end = value
            interval_length = parser.isoparse(interval_end .replace(" ", "+", 1)) - \
                              parser.isoparse(interval_start.replace(" ", "+", 1))
            if interval_length > timedelta(hours=1):
                value = add_1hr_to_iso(interval_start)

            else:
                pass
            query_suffix = query_suffix + '{} <= "{}" AND {} < "{}" AND '\
                .format('service_date',
                        iso_to_datestr(value),
                        'timestamp',
                        parser.isoparse(value.replace(" ", "+", 1))
                        )

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
        geojson['features'].append(feature)
    return geojson


#--------------- API ---------------

class LiveMap(Resource):
    def get(self):
        import geojson
        with open('./api-www/static/lastknownpositions.geojson', 'r') as infile:
            return geojson.load(infile)

class RouteQuerySchema(Schema):
    route_short = fields.Str(required=True)
    start = fields.Str(required=True)  # in ISO 8601 e.g. 2020-08-11T14:42:00+00:00
    end = fields.Str(required=True)  # in ISO 8601 e.g. 2020-08-11T15:12:00+00:00
    output = fields.Str(required=True)

route_schema = RouteQuerySchema()

class SystemAPI(Resource):
    def get(self):
        errors = route_schema.validate(request.args)
        if errors:
            abort(400, str(errors))
        conn = db_connect.connect()
        query_prefix = "SELECT * FROM buses WHERE {}"
        query_suffix = query_builder(request.args)
        query_compound = query_prefix.format(query_suffix )
        print (query_compound)
        query = conn.execute(query_compound)
        results = {'observations': unpack_query_results(query)}

        if request.args['output'] == 'geojson':
            print(str(len(results['observations'])) + ' positions returned via API')
            return results_to_FeatureCollection(results)
        else: #only geojson for now
            return {'API error':'incorrect output format type. only "geojson" supported at the time'}


#--- ENDPOINTS ---#

#----for map----#
api.add_resource(LiveMap, '/api/v1/nyc/livemap')

#----for apps----#
# /api/v1/nyc/buses?output=geojson&route_short=Bx4&start=2021-03-28T00:00:00+00:00&end=2021-03-28T01:00:00+00:00
# gives out all positions on a given 'route_short' during the specific interval in ISO 8601 format
# from 'start'
# to 'end'
# max interval = 1 hour
# output=geojson for now
api.add_resource(SystemAPI, '/api/v1/nyc/buses', endpoint='buses')


#-----------------------------------------------------------------------------------------



# is the browser s# @app.route('/')
# # def index():
# #     return render_template('index.html')
# #
# # @app.route('/map')
# # def map():
# #     return render_template('map.html')
# #
# # @app.route('/occupancy')
# # def occupancy():
# #     return render_template('occupancy.html')
# #
# # @app.route('/faq')
# # def faq():
# #     return render_template('faq.html')till caching this — wrap this in an http header to expire it?
# @app.route('/api/v1/nyc/lastknownpositions')
# def lkp():
#     print (app.static_folder)
#     return send_from_directory(app.static_folder,'lastknownpositions.geojson')

if __name__ == '__main__':
    app.run(debug=True)
