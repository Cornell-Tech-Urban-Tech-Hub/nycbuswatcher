from fastapi import FastAPI, Request
import uvicorn
from sqlalchemy import create_engine

import Database as db
from APIhelpers import *

from dotenv import load_dotenv
load_dotenv()
from config import config
api_url_stem="/api/v1/nyc/livemap"

#-----------------------------------------------------------------------------------------
# sources
# fastapi implementation after tutorial https://fastapi.tiangolo.com/tutorial/query-params/
# query parameter handling after https://stackoverflow.com/questions/30779584/flask-restful-passing-parameters-to-get-request
#-----------------------------------------------------------------------------------------


#--------------- INITIALIZATION ---------------

db_connect = create_engine(db.get_db_url(config.config['dbuser'], config.config['dbpassword'], config.config[
    'dbhost'], config.config['dbport'], config.config['dbname']))
# to 'localhost' for debugging?

app = FastAPI()


#-------------- Fast API -------------------------------------------------------------
@app.get("/")
async def root():
    return {"message": "NYCBuswatcher API v2"}


@app.get("/api/v2/nyc/livemap")
async def fetch_livemap():
    import geojson
    with open('./api/static/lastknownpositions.geojson', 'r') as infile:
        return geojson.load(infile)

# /api/v2/nyc/buses?route_short=Bx4&start=2021-03-28T00:00:00+00:00&end=2021-03-28T01:00:00+00:00


# todo add validatiopn for these requests (using similar filters as the flask_restful v1 api
# https://fastapi.tiangolo.com/tutorial/query-params-str-validations/

@app.get("/api/v2/nyc/buses")
# per https://stackoverflow.com/questions/62279710/fastapi-variable-query-parameters
async def fetch_snapshot(request: Request, route_short: str, start: str, end: str):
    conn = db_connect.connect()
    query_prefix = "SELECT * FROM buses WHERE {}"
    query_suffix = query_builder(request.query_params)
    query_compound = query_prefix.format(query_suffix )
    print (query_compound)
    query = conn.execute(query_compound)
    results = {'observations': unpack_query_results(query)}
    print(str(len(results['observations'])) + ' positions returned via API')
    return results_to_FeatureCollection(results)


if __name__ == '__main__':
    uvicorn.run(app,debug=True)
