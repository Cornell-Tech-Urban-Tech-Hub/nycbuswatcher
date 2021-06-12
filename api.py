from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn
from dotenv import load_dotenv
from datetime import datetime
from pydantic import BaseModel
import shared.DataStructures as data
import json
from shared.Helpers import timeit
#-----------------------------------------------------------------------------------------
# fastapi implementation after tutorial https://fastapi.tiangolo.com/tutorial/query-params/
# query parameter handling after https://stackoverflow.com/questions/30779584/flask-restful-passing-parameters-to-get-request
#-----------------------------------------------------------------------------------------

#--------------- INITIALIZATION ---------------
load_dotenv()
from shared.config import config
api_url_stem="/api/v2/nyc/"

app = FastAPI()


# add CORS stuff https://fastapi.tiangolo.com/tutorial/cors/
# add auth/key registry (3rd party plugin? for API control)
#-------------- Fast API -------------------------------------------------------------


@app.get('/')
async def root():
    return {'message': 'NYCBuswatcher API v2'}


@timeit
def make_store():
    print ('i recreated the DataStore()') #bug how to refresh the DataStore to include new Shipments while the api is running as a daemon? or is it unnecessary?
    return data.DataStore()

#todo debug this
@app.get('/api/v2/nyc/routes/{year}/{month}/{day}/{hour}')
async def list_routes(year,month,day,hour):#todo add validators
    store = make_store() #bug this might be costly for each response? but how else to refresh it outside the function
    date_pointer=data.DatePointer(datetime(year=int(year),month=int(month),day=int(day),hour=int(hour)))
    routes = store.list_routes(date_pointer)
    return {"message": "This will provide a JSON formatted list of routes available for a given date_pointer",
            "routes": json.dumps(routes)}

# todo ENDPOINT get json array of the URLs for entire city for one hour
# @app.get('/api/v2/nyc/citywide/{year}/{month}/{day}/{hour}')
# async def list_routes(year,month,day,hour):#todo add validators
#     date_pointer=datetime.datetime(year=year,month=month,day=day,hour=hour)
#     return {"message": "This will provide a JSON formatted list of routes available for a given date_pointer",
#             "date_pointer": date_pointer}

# todo ENDPOINT get json array of all the URLS for one route for a whole day
# @app.get('/api/v2/nyc/citywide/{year}/{month}/{day}/{hour}')
# async def list_routes(year,month,day,hour):#todo add validators
#     date_pointer=datetime.datetime(year=year,month=month,day=day,hour=hour)
#     return {"message": "This will provide a JSON formatted list of routes available for a given date_pointer",
#             "date_pointer": date_pointer}

# ENDPOINT get a single Shipment by date_pointer
#per https://fastapi.tiangolo.com/advanced/additional-responses/#additional-media-types-for-the-main-response
class Shipment(BaseModel):
    id: str
    value: str
@app.get('/api/v2/nyc/buses/{year}/{month}/{day}/{hour}/{route}',
    response_model=Shipment,
         responses={
             200: {
                 'content': {'application/json': {}},
                 'description': 'Return the JSON item or an image.',
             }
         },
         )
async def fetch_Shipment(year,month,day,hour,route): #todo add validators
    shipment_to_get = 'data/store/shipments/{}/{}/{}/{}/shipment_{}_{}-{}-{}-{}.json'.format(year,month,day,hour,route,year,month,day,hour)
    return FileResponse(shipment_to_get, media_type="application/json")
    # except:
    #     return {"message": "We couldn't find that file!",
    #             "date_pointer": (year,month,day,hour,route)}


@app.get('/api/v2/nyc/dashboard')
async def list_routes():
    return {"message": "dashboard report title",
            "data": "This is where the data will go, and the dashboard will display it!"}



if __name__ == '__main__':
    uvicorn.run(app, port=5000, debug=True)
