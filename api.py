from datetime import datetime
from fastapi import FastAPI, Query, Path
import os

from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import argparse
import logging
from starlette.responses import Response

from common.Models import DateRoutePointer, PrettyJSONResponse, MongoLake
from dotenv import load_dotenv


#--------------- INITIALIZATION ---------------
load_dotenv()
environment = os.environ['PYTHON_ENV']
api_url_stem="/nyc/"
app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


#######################
# ENDPOINTS
#######################

# All Buses In History For Route
@app.get('/nyc/{route}/history',response_class=PrettyJSONResponse)
async def get_all_buses_on_route_history(
        route: str = Query("M15", max_length=6)):

    content = MongoLake(environment).get_all_buses_on_route_history(route)
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

    content = MongoLake(environment).get_all_buses_on_route_single_hour(date_route_pointer)

    return Response(content, media_type='application/json')



# todo this code doesnt execute in the docker environment
# main -----------------------------------------------------------------------------------------------------
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='NYCbuswatcher v2.1 API, mongodb version')
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
