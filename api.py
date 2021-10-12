from datetime import datetime
from fastapi import FastAPI, Query, Path

from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import argparse
import logging
from starlette.responses import Response

from common.Models import DateRoutePointer
from dotenv import load_dotenv
from common.Helpers import PrettyJSONResponse
from common.Models import MongoLake

#--------------- INITIALIZATION ---------------
load_dotenv()
api_url_stem="/api/v2/nyc/"
app = FastAPI()
templates = Jinja2Templates(directory="assets/templates")

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


#######################
# SHIPMENT ENDPOINTS
#######################


# All Buses In History For Route
@app.get('/nyc/{route}/history',response_class=PrettyJSONResponse)
async def get_all_buses_on_route_history(
        route: str = Query("M15", max_length=6)):

    content = MongoLake().get_all_buses_on_route_history(route)
    return Response(content, media_type='application/json')

# All Buses In Hour For Route
@app.get('/nyc/{year}/{month}/{day}/{hour}/{route}/buses',response_class=PrettyJSONResponse)
async def get_all_buses_on_route_single_hour(
        *,
        year: int = Path(..., ge=2020, le=2050),
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

    content = MongoLake().get_all_buses_on_route_single_hour(date_route_pointer)

    return Response(content, media_type='application/json')






# # Fetch Single Shipment As geoJSON
# @app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses/geojson',response_class=PrettyJSONResponse)
# async def fetch_single_Shipment_as_geoJSON(
#         *,
#         year: int = Path(..., ge=2020, le=2050),
#         month: int = Path(..., ge=1, le=12),
#         day: int = Path(..., ge=1, le=31),
#         hour: int = Path(..., ge=0, le=23),
#         route: str = Path(..., max_length=6)
#         ):
#     date_route_pointer=DateRoutePointer(datetime(year=int(year),
#                                                  month=int(month),
#                                                  day=int(day),
#                                                  hour=int(hour)),
#                                         route.upper())
#     data = Shipment(pathlib.Path.cwd(),date_route_pointer).to_FeatureCollection()
#     return data
#     # return Response(content=data, media_type="application/json")

#
# ######################
# # GLACIER ENDPOINTS
# ######################
#
#
# # List All Glaciers In History For Route
# @app.get('/api/v2/nyc/{route}/glaciers',response_class=PrettyJSONResponse)
# async def list_all_glaciers_in_history_for_route(
#         route: str = Query("M15", max_length=6)):
#
#     glacier_index_to_get = f'data/lake/glaciers/indexes/glacier_index_{route.upper()}.json'
#     if not isfile(glacier_index_to_get):
#         return Response(status_code=404)
#     with open(glacier_index_to_get) as f:
#         content = f.read()
#     return Response(content, media_type='application/json')
#
# # Fetch Single Glacier as Download
# @app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/archive')
# # after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
# async def fetch_single_glacier(
#         *,
#         year: int = Path(..., ge=2020, le=2050),
#         month: int = Path(..., ge=1, le=12),
#         day: int = Path(..., ge=1, le=31),
#         hour: int = Path(..., ge=0, le=23),
#         route: str = Path(..., max_length=6)
# ):
#     filename = f'glacier_{year}-{month}-{day}-{hour}-{route.upper()}.tar.gz'
#     glacier_to_get = f'data/lake/glaciers/{year}/{month}/{day}/{hour}/{route.upper()}/{filename}'
#     if not isfile(glacier_to_get):
#         return Response(status_code=404)
#
#     return FileResponse(glacier_to_get, filename=filename)
#
# '''
# #######################
# # ROUTE ARCHIVE ENDPOINT
# #######################
#
# # Fetch An Entire Route's History as a Tarball—Glaciers and Shipments
# @app.get('/api/v2/nyc/{route}/history')
# # after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
# async def fetch_route_history(
#         route: str = Query("M15", max_length=6)
# ):
#
#
#     # # trigger creation of RouteHistory
#     # RouteHistory(pathlib.Path.cwd(), route)
#
#     # create the pointers
#     filename = f'route_history_{route.upper()}.tar.gz'
#     route_history_to_get = f'data/history/{filename}'
#
#     if not isfile(route_history_to_get):
#         return Response(status_code=404)
#
#     return FileResponse(route_history_to_get, filename=filename)
# '''




# main -----------------------------------------------------------------------------------------------------
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='NYCbuswatcher v2 API, mongodb version')
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
