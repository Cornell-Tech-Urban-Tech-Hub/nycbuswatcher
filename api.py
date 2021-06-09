from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn
from shared.API import *
from dotenv import load_dotenv
import datetime
from pydantic import BaseModel

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


@app.get('/api/v2/nyc/routes/{year}/{month}/{day}/{hour}')
async def list_routes(year,month,day,hour):#todo add validators
    date_pointer=datetime.datetime(year=year,month=month,day=day,hour=hour)
    return {"message": "This will provide a JSON formatted list of routes available for a given date_pointer",
            "date_pointer": date_pointer}



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


if __name__ == '__main__':
    uvicorn.run(app, port=5000, debug=True)
