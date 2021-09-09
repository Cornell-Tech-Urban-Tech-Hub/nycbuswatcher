from datetime import datetime
from os.path import isfile
from fastapi import FastAPI, Query, Path

from fastapi.templating import Jinja2Templates
import uvicorn
import argparse
import logging
import pathlib
import inspect
from starlette.responses import Response
from starlette.responses import FileResponse
from common.Models import DateRoutePointer, Shipment
from dotenv import load_dotenv
from common.Helpers import PrettyJSONResponse

#--------------- INITIALIZATION ---------------
load_dotenv()
api_url_stem="/api/v2/nyc/"
app = FastAPI()
templates = Jinja2Templates(directory="assets/templates")


#######################
# SHIPMENT ENDPOINTS
#######################

# List All Shipments In History For Route
@app.get('/api/v2/nyc/{route}',response_class=PrettyJSONResponse)
async def list_all_shipments_in_history_for_route(
        route: str = Query("M15", max_length=6)):

    shipment_to_get = f'data/store/shipments/indexes/shipment_index_{route.upper()}.json'
    if not isfile(shipment_to_get):
        return Response(status_code=404)
    with open(shipment_to_get) as f:
        content = f.read()
    return Response(content, media_type='application/json')


# Fetch Single Shipment As JSON
@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses')
# after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
async def fetch_single_shipment(
        *,
        year: int = Path(..., ge=2020, le=2050),
        month: int = Path(..., ge=1, le=12),
        day: int = Path(..., ge=1, le=31),
        hour: int = Path(..., ge=0, le=23),
        route: str = Path(..., max_length=6)
):
    shipment_to_get = 'data/store/shipments/{}/{}/{}/{}/{}/shipment_{}-{}-{}-{}-{}.json'. \
        format(year,month,day,hour,route.upper(),year,month,day,hour,route.upper())
    if not isfile(shipment_to_get):
        return Response(status_code=404)
    with open(shipment_to_get) as f:
        content = f.read()
    return Response(content, media_type='application/json')


# Fetch Single Shipment As geoJSON
@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses/geojson',response_class=PrettyJSONResponse)
async def fetch_single_Shipment_as_geoJSON(
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
    return Shipment(pathlib.Path.cwd(),date_route_pointer).to_FeatureCollection()



######################
# GLACIER ENDPOINTS
######################


# Fetch Single Glacier as Download
@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/archive')
# after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
async def fetch_single_glacier(
        *,
        year: int = Path(..., ge=2020, le=2050),
        month: int = Path(..., ge=1, le=12),
        day: int = Path(..., ge=1, le=31),
        hour: int = Path(..., ge=0, le=23),
        route: str = Path(..., max_length=6)
):
    filename = f'glacier_{year}-{month}-{day}-{hour}-{route.upper()}.tar.gz'
    glacier_to_get = f'data/lake/glaciers/{year}/{month}/{day}/{hour}/{route.upper()}/{filename}'
    if not isfile(glacier_to_get):
        return Response(status_code=404)
    # with open(glacier_to_get, "rb") as f:
    #     content = f.read()
    # return Response(content, media_type='application/gzip')
    return FileResponse(glacier_to_get, filename=filename)


# main -----------------------------------------------------------------------------------------------------
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
