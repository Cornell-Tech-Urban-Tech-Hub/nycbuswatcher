from datetime import datetime
from os.path import isfile
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import uvicorn

from shared.Models import DatePointer, DateRoutePointer, DataStore, Shipment
from shared.Helpers import timeit


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

app.mount("/static", StaticFiles(directory="assets"), name="static")


# add CORS stuff https://fastapi.tiangolo.com/tutorial/cors/
# add auth/key registry (3rd party plugin? for API control)
#-------------- Fast API -------------------------------------------------------------


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


@timeit #bug too costly?
def make_store(): #bug how to automate this for refresh periodically
    print ('i recreated the DataStore()')
    return DataStore()


#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /
# FUNCTION Displays documentation
@app.get('/', response_class=HTMLResponse)
async def discover_endpoints(request: Request):
    # return templates.TemplateResponse("index.html", {"request": request})
    return RedirectResponse("/docs")

#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/dashboard
# FUNCTION Dashboard metadata shpwing number of barrels and shipments per hour per route currently stored.
@app.get('/api/v2/nyc/dashboard')
# after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
async def send_dashboard():
    filename='data/dashboard.csv'
    if not isfile(filename):
        return Response(status_code=404)
    with open(filename) as f:
        content = f.read()
    return Response(content, media_type='text/csv')


# V3 MAIN RESPONSE ENDPOINT — STATIC
#------------------------------------------------------------------------------------------------------------------------

@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses')
# after https://stackoverflow.com/questions/62455652/how-to-serve-static-files-in-fastapi
async def fetch_shipment(year,month,day,hour,route):
    shipment_to_get = 'data/store/shipments/{}/{}/{}/{}/{}/shipment_{}-{}-{}-{}-{}.json'.format(year,month,day,hour,route,year,month,day,hour,route)
    if not isfile(shipment_to_get):
        return Response(status_code=404)
    with open(shipment_to_get) as f:
        content = f.read()
    return Response(content, media_type='application/json')

# # V1 MAIN RESPONSE ENDPOINT (using Shipment class = elegant but creates empty folders)
# #------------------------------------------------------------------------------------------------------------------------
# # ENDPOINT /api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses
# # FUNCTION get a single Shipment by date_pointer as flat JSON 'buses' array
# @app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses',response_class=PrettyJSONResponse)
# async def fetch_Shipment(year,month,day,hour,route):
#     date_route_pointer=DateRoutePointer(datetime(year=int(year),
#                                                  month=int(month),
#                                                  day=int(day),
#                                                  hour=int(hour)),
#                                         route)
#
#     #bug need to check if the shipment exists before instantiating object, otherwise it creates an empty folder because of GenericFolder inheritance
#     return Shipment(date_route_pointer).load_file()

# # V2 MAIN RESPONSE ENDPOINT — STATIC, NOT SO SIMPLE, FASTER?
# #------------------------------------------------------------------------------------------------------------------------
# # ENDPOINT /api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses
# # FUNCTION get a single Shipment by date_pointer as flat JSON 'buses' array
# #per https://fastapi.tiangolo.com/advanced/additional-responses/#additional-media-types-for-the-main-response
# class IndentedJSON(BaseModel):
#     id: str
#     value: str
# @app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses',
#     response_model=IndentedJSON,
#          responses={200: {
#                  'content': {'application/json': {}},
#                  'description': 'Return the JSON item or an image.',
#              }
#          },
#          )
# async def fetch_shipment(year,month,day,hour,route):
#     shipment_to_get = 'data/store/shipments/{}/{}/{}/{}/{}/shipment_{}-{}-{}-{}-{}.json'.format(year,month,day,hour,route,year,month,day,hour,route)
#     return FileResponse(shipment_to_get, media_type="application/json")



#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/buses/{year}/{month}/{day}/{hour}/{route}/geojson
# FUNCTION get a single Shipment by date_pointer as geoJSON FeatureCollection
@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/{route}/buses/geojson',response_class=PrettyJSONResponse)
async def fetch_Shipment_as_geoJSON(year,month,day,hour,route):
    date_route_pointer=DateRoutePointer(datetime(year=int(year),
                                                 month=int(month),
                                                 day=int(day),
                                                 hour=int(hour)),
                                        route)
    return Shipment(date_route_pointer).to_FeatureCollection()


#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/{year}/{month}/{day}/{hour}/routes
# FUNCTION List routes with shipment data for period specified.

@app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/routes',response_class=PrettyJSONResponse)
async def list_routes(year,month,day,hour):
    store = make_store()
    date_pointer=DatePointer(datetime(year=int(year),month=int(month),day=int(day),hour=int(hour)))
    routes = store.list_routes_in_store(date_pointer)
    result = {"year":year,
              "month":month,
              "day":day,
              "hour":hour,
              "routes": routes}
    return result


# # todo ENDPOINTs citywide shipment URLS for 1 month, 1 day, 1 hour
# #------------------------------------------------------------------------------------------------------------------------
# # ENDPOINT /api/v2/nyc/{year}/{month}/{day}/{hour}/urls/all
# # FUNCTION get json array of the Shipment URLs for entire city for one hour
# @app.get('/api/v2/nyc/{year}/{month}/{day}/{hour}/urls/all')
# async def get_urls_hour_all(year,month,day,hour):
#     date_pointer=DatePointer(datetime(year=year, month=month, day=day, hour=hour))
#
#     return json.dumps(
#         Shipment(date_pointer).to_FeatureCollection()
#     )


#------------------------------------------------------------------------------------------------------------------------
# ENDPOINT /api/v2/nyc/{year}/{month}/{day}/urls/all
# ENDPOINT /api/v2/nyc/{year}/{month}/urls/all

#------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    uvicorn.run(app, port=5000, debug=True)
