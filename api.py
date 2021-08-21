from datetime import datetime
from os.path import isfile
from fastapi import FastAPI, Request, Query, Path
from typing import Optional
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pickle
import uvicorn
import argparse
import logging
import pathlib

from common.Models import DatePointer, DateRoutePointer, DataStore, Shipment

from dotenv import load_dotenv

#-----------------------------------------------------------------------------------------
# fastapi implementation after tutorial https://fastapi.tiangolo.com/tutorial/query-params/
# query parameter handling after https://stackoverflow.com/questions/30779584/flask-restful-passing-parameters-to-get-request
#-----------------------------------------------------------------------------------------

#--------------- INITIALIZATION ---------------
load_dotenv()
api_url_stem="/api/v2/nyc/"

app = FastAPI()
templates = Jinja2Templates(directory="assets/templates")

#todo this isn't loading in dashboard because it keeps try to get it on port 8000 not 5000?
app.mount("/assets", StaticFiles(directory="assets"), name="assets")



#-------------- Pretty JSON -------------------------------------------------------------
# https://gitter.im/tiangolo/fastapi?at=5d381c558fe53b671dc9aa80
import json
import typing

from starlette.responses import Response

class PrettyJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=4,
            separators=(", ", ": "),
        ).encode("utf-8")

#-------------- debugging and testing -------------------------------------------------------------
import functools
import time

def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        logging.warning(f"Elapsed time: {elapsed_time:0.4f} seconds")
        return value
    return wrapper_timer

#-------------- Initialize Data Store -------------------------------------------------------------
#bug find a way to not do this with each request, but every 5 min?
@timer
def reload_store():
    try:
        with open('data/store/DataStore.pickle', 'rb') as f:
            logging.warning('i LOADED existing DataStore.pickle')
            return pickle.load(f)
    except:
        store = DataStore(pathlib.Path.cwd()).pickle_myself()
        logging.warning('i REBUILT DataStore.pickle')
        with open('data/store/DataStore.pickle', 'rb') as f:
            return pickle.load(f)

#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/{route}
# FUNCTION List All Shipments In History For Route
@app.get('/api/v2/nyc/{route}',response_class=PrettyJSONResponse)
async def list_all_shipments_in_history_for_route(
        route: str = Query("M15", max_length=6)):

    logging.debug('calling reload_store()')
    store = reload_store()

    route_shipments = store.find_route_shipments(route.upper())
    shipments = [
        {"route": s.date_pointer.route,
         "year":s.date_pointer.year,
         "month":s.date_pointer.month,
         "day":s.date_pointer.day,
         "hour":s.date_pointer.hour,
         "url":s.url
         } for s in route_shipments
    ]
    shipments_sorted = sorted(shipments, key = lambda i: (i['year'],i['month'],i['day'],i['hour']))
    return {"route":route.upper(),
            "shipments": shipments_sorted}


#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/{year}/{month}/{day}/{hour}/routes
# FUNCTION List routes with shipment data for period specified.
@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/routes',response_class=PrettyJSONResponse)
async def list_all_routes_for_hour(
        *,
        year: int = Path(..., ge=2020, le=2050), #future populate these #s based on DataStore's metadata about what's in the data (e.g. prevent a request for something that isn't there
        month: int = Path(..., ge=1, le=12),
        day: int = Path(..., ge=1, le=31),
        hour: int = Path(..., ge=0, le=23)
):

    logging.debug('calling reload_store()')
    store = reload_store()

    date_pointer=DatePointer(datetime(year=int(year),month=int(month),day=int(day),hour=int(hour)))
    routes = sorted(store.list_routes_in_store(date_pointer))
    result = {"year":year,
              "month":month,
              "day":day,
              "hour":hour,
              "routes": routes}
    return result


# #------------------------------------------------------------------------------------------------------------------------
# # ENDPOINT /api/v2/nyc/buses/{year}/{month}/{day}/{hour}/{route}/json
# # FUNCTION get a single Shipment by date_pointer as JSON
# @app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses')
# # after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
# async def fetch_single_shipment(*,
#         year: int = Path(..., ge=2020, le=2050), #future populate these #s based on DataStore's metadata about what's in the data (e.g. prevent a request for something that isn't there
#         month: int = Path(..., ge=1, le=12),
#         day: int = Path(..., ge=1, le=31),
#         hour: int = Path(..., ge=0, le=23),
#         route: str = Path(..., max_length=6)
# ):
#     shipment_to_get = 'data/store/shipments/{}/{}/{}/{}/{}/shipment_{}-{}-{}-{}-{}.json'.\
#         format(year,month,day,hour,route.upper(),year,month,day,hour,route.upper())
#     if not isfile(shipment_to_get):
#         return Response(status_code=404)
#     with open(shipment_to_get) as f:
#         content = f.read()
#     return Response(content, media_type='application/json')


#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/buses/{year}/{month}/{day}/{hour}/{route}/geojson
# FUNCTION get a single Shipment by date_pointer as geoJSON FeatureCollection
@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses/geojson',response_class=PrettyJSONResponse)
async def fetch_single_Shipment_as_geoJSON(
        *,
        year: int = Path(..., ge=2020, le=2050), #future populate these #s based on DataStore's metadata about what's in the data (e.g. prevent a request for something that isn't there
        month: int = Path(..., ge=1, le=12),
        day: int = Path(..., ge=1, le=31),
        hour: int = Path(..., ge=0, le=23),
        route: str = Path(..., max_length=6)
):
    date_route_pointer=DateRoutePointer(datetime(year=int(year),
                                                 month=int(month),
                                                 day=int(day),
                                                 hour=int(hour)),
                                        route.upper())
    return Shipment(pathlib.Path.cwd(),date_route_pointer).to_FeatureCollection()


#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/shipments?route=BX4&year=2021&month=6&day=10&hour=4
# FUNCTION List shipments available for a given set of arguments
@app.get('/api/v2/shipments',response_class=PrettyJSONResponse)
async def list_shipments_by_query_all_fields_optional(
        year: Optional[int] = Query(None, ge=2020, le=2050), #future populate these #s based on DataStore's metadata about what's in the data (e.g. prevent a request for something that isn't there
        month: Optional[int] = Query(None, ge=1, le=12),
        day: Optional[int] = Query(None, ge=1, le=31),
        hour: Optional[int] = Query(None, ge=0, le=23),
        route: Optional[str] = Query(None, max_length=6)
):

    logging.debug('calling reload_store()')
    store = reload_store()

    if route:
        route = route.upper()

    params = {
        'route':route,
        'year':year,
        'month':month,
        'day':day,
        'hour':hour
    }
    shipments = store.find_query_shipments(params)
    return {"query":params,
            "shipments": shipments}


# #------------------------------------------------------------------------------------------------------------------------
# # ENDPOINT /api/v2/nyc/dashboard
# # FUNCTION Dashboard metadata showing number of barrels and shipments per hour per route currently stored.
# @app.get('/api/v2/nyc/dashboard/csv')
# # after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
# async def fetch_dashboard_data():
#     filename= 'data/dashboard.csv'
#     if not isfile(filename):
#         logging.debug('didnt find the dashboard.csv')
#         return Response(status_code=404)
#     with open(filename) as f:
#         content = f.read()
#         return Response(content, media_type='text/csv')
#

# #------------------------------------------------------------------------------------------------------------------------
# # ENDPOINT /
# # FUNCTION Displays documentation
# @app.get('/', response_class=HTMLResponse)
# async def discover_endpoints(request: Request):
#     # return templates.TemplateResponse("index.html", {"request": request})
#     return RedirectResponse("/docs")



#------------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='NYCbuswatcher v2 API')
    parser.add_argument("-v",
                        "--verbose",
                        help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    uvicorn.run(app, port=5000, debug=True)
