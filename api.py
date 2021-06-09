from fastapi import FastAPI, Request, Query
from fastapi.staticfiles import StaticFiles
import uvicorn
from shared.API import *
from dotenv import load_dotenv
import datetime

#-----------------------------------------------------------------------------------------
# fastapi implementation after tutorial https://fastapi.tiangolo.com/tutorial/query-params/
# query parameter handling after https://stackoverflow.com/questions/30779584/flask-restful-passing-parameters-to-get-request
#-----------------------------------------------------------------------------------------

#--------------- INITIALIZATION ---------------
load_dotenv()
from shared.config import config
api_url_stem="/api/v2/nyc/"

app = FastAPI()

# app.mount("/static", StaticFiles(directory="static/"), name="static")
app.mount("/assets", StaticFiles(directory="assets/"), name="static")

# add CORS stuff https://fastapi.tiangolo.com/tutorial/cors/
# add auth/key registry (3rd party plugin? for API control)
#-------------- Fast API -------------------------------------------------------------


@app.get("/")
async def root():
    return {"message": "NYCBuswatcher API v2"}


@app.get("/api/v2/nyc/routes/{year}/{month}/{day}/{hour}")
async def list_routes(
                        request: Request,
                        year: int = Query (None,
                                           min_length=4,
                                           max_length=4),
                        month: int = Query (None,
                                            min_length=4,
                                            max_length=4),
                        day: int = Query (None,
                                          min_length=4,
                                          max_length=4),
                        hour: int = Query (None,
                                           min_length=4,
                                           max_length=4)
                        ):
    date_pointer=datetime.datetime(year=year,month=month,day=day,hour=hour)
    return {"message": "This will provide a JSON formatted list of routes available for a given date_pointer and kind"}


@app.get("/api/v2/nyc/buses/{year}/{month}/{day}/{hour}/{route}")
# per https://stackoverflow.com/questions/62279710/fastapi-variable-query-parameters
async def fetch_Shipment(
                        request: Request,
                        year: int = Query (None,
                                            min_length=4,
                                            max_length=4),
                        month: int = Query (None,
                                           min_length=4,
                                           max_length=4),
                        day: int = Query (None,
                                           min_length=4,
                                           max_length=4),
                        hour: int = Query (None,
                                           min_length=4,
                                           max_length=4),
                        route: str = Query (None,
                                            min_length=2,
                                            max_length=6)
                        ):

    return {"message": "This will serve a static JSON Shipment for for a given date_pointer"}


if __name__ == '__main__':
    uvicorn.run(app, port=5000, debug=True)
