import os
import pickle
from dateutil import parser
from decimal import Decimal

from datetime import date, datetime, timedelta
from pathlib import Path, PurePath
from uuid import uuid4

from pymongo import MongoClient
from bson import json_util
from bson.json_util import dumps

import datetime
import requests
from time import time
import datetime as dt
import trio

import common.config.config as config


#### grabber code ################################################################################################################

def async_grab_and_store(environment):

    def get_OBA_routelist():
        url = "http://bustime.mta.info/api/where/routes-for-agency/MTA%20NYCT.json?key=" + os.getenv("API_KEY")
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 503: # response is bad, so go to exception and load the pickle
                raise Exception(503, "503 error code fetching route definitions. OneBusAway API probably overloaded.")
            else: # response is good, so save it to pickle and proceed
                with open(('data/routes-for-agency.pickle'), "wb") as pickle_file:
                    pickle.dump(response,pickle_file)
        except Exception as e: # response is bad, so load the last good pickle
            with open(('data/routes-for-agency.pickle'), "rb") as pickle_file:
                response = pickle.load(pickle_file)
            logging.debug("Route URLs loaded from pickle cache.")
        finally:
            routes = response.json()
        return routes

    def get_SIRI_request_urlpaths():
        SIRI_request_urlpaths = []
        routes=get_OBA_routelist()
        for route in routes['data']['list']:
            SIRI_request_urlpaths.append({route['id']:"/api/siri/vehicle-monitoring.json?key={}&VehicleMonitoringDetailLevel=calls&LineRef={}".format(os.getenv("API_KEY"), route['id'])})
        return SIRI_request_urlpaths

    def num_buses(feeds):
        num_buses=0
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                try:
                    route_data = route_data.json()
                    for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                        num_buses = num_buses + 1
                except: # no vehicle activity?
                    pass
        return num_buses


    # main function
    start = time()
    SIRI_request_urlpaths = get_SIRI_request_urlpaths()
    feeds = []

    async def grabber(s,a_path,route_id):
        try:
            r = await s.get(path=a_path, retries=2, timeout=30)
            feeds.append({route_id:r})
        except Exception:
            logging.error (f'\t{datetime.datetime.now()}\tTimeout or too many retries for {route_id}.')

    async def main(path_list):
        from asks.sessions import Session
        s = Session('http://bustime.mta.info', connections=config.config['http_connections'])
        async with trio.open_nursery() as n:
            for path_bundle in path_list:
                for route_id,path in path_bundle.items():
                    n.start_soon(grabber, s, path, route_id )

    trio.run(main, SIRI_request_urlpaths)

    MongoLake(environment).store_feeds(feeds)

    # report results to console
    n_buses = num_buses(feeds)
    end = time()
    logging.info('Saved {} route feeds tracking {} buses in {:2f} seconds at {}.'.format(len(feeds),n_buses,(end - start), dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return


# with help from https://realpython.com/introduction-to-mongodb-and-python/
class MongoLake():

    def __init__(self, environment):
        self.uid = uuid4().hex
        self.environment = environment

    def get_mongo_client(self):
        return MongoClient(host=config.config['db_host'], port=27017)

    def store_feeds(self, feeds):
        with self.get_mongo_client() as client:
            db = client.nycbuswatcher # db name is 'buswatcher'
            response_db = db["siri_archive"] # raw responses
            buses_db = db["buses"] # only the ['MonitoredVehicleJourney'] dicts

            # iterate over each route
            for route_report in feeds:
                for route_id, response in route_report.items():

                    # # dump the response to archive
                    # response_db.insert_one(response.json())

                    # make a dict with the response
                    response_json = response.json()

                    # parse and dump
                    try:
                        vehicle_activity = response_json['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']
                        logging.debug(f"{route_id} has {len(vehicle_activity)} buses.")

                        buses=[]
                        for v in vehicle_activity:

                            # copy and cast the time into the MonitoredVehicleJourney dict
                            #todo pick one of these and remove the other
                            v['MonitoredVehicleJourney']['RecordedAtTime'] = parser.isoparse(v['RecordedAtTime'])
                            v['RecordedAtTimeDatetime'] = parser.isoparse(v['RecordedAtTime'])

                            # and remove it from the VehicleActivity dict
                            # v.pop("RecordedAtTime", None) #bug this raises key errors even though a default value is provided

                            # remove ['MonitoredVehicleJourney']['OnwardCalls'] dict
                            v.get("MonitoredVehicleJourney", {}).pop("OnwardCalls", None)

                            logging.debug(f"Bus {v['MonitoredVehicleJourney']['VehicleRef'] } recorded at {v['RecordedAtTime']}")
                            buses.append(v)

                        buses_db.insert_many(buses)

                    except KeyError as e:
                        # future find a way to filter these out to reduce overhead
                        # this is almost always the result of a route that doesn't exist, so why is it in the OBA response?
                        logging.debug(f"SIRI API response for {route_id} was {type(e)} {e}")
                        pass

                    except json.JSONDecodeError:
                        # this is probably no response?
                        logging.debug(f'SIRI API response for {route_id} was probably = NO VEHICLE ACTIVITY.')
                        pass

        return


    # query db for all buses on a route in history
    def get_all_buses_on_route_history(self, passed_route):

        with self.get_mongo_client() as client:
            db = client.nycbuswatcher # db name is 'buswatcher'
            Collection = db["buses"] # collection name is 'buses'

            lineref_prefix = "MTA NYCT_"
            lineref = lineref_prefix + passed_route

            criteria = { "MonitoredVehicleJourney.LineRef" : lineref }

            #todo sort these
            payload = json.loads(dumps(Collection.find(criteria)))

            #todo encapsulate this as geojson
            #bug this dumps the MonitoredVehicleJourney / RecordedAtTime as "RecordedAtTime": { "$date": 1634002375000}
            # make a json out of it
            response = { "scope": "history",
                         "query": {
                             "lineref": lineref
                         },
                         "payload": payload
                         }

            return json_util.dumps(response, indent=4)


    # query db for all buses on a route for a specific hour (like the old Shipment)
    def get_all_buses_on_route_single_hour(self, date_route_pointer):

        with self.get_mongo_client() as client:
            db = client.nycbuswatcher # db name is 'buswatcher'
            Collection = db["buses"] # collection name is 'buses'

            lineref_prefix = "MTA NYCT_"
            lineref = lineref_prefix + date_route_pointer.route

            # https://medium.com/nerd-for-tech/how-to-prepare-a-python-date-object-to-be-inserted-into-mongodb-and-run-queries-by-dates-and-range-bc0da03ea0b2

            # build time criteria


            #bug date queries are not working
            # from_date = date_route_pointer.timestamp
            # to_date = date_route_pointer.timestamp + timedelta(hours = 1)
            # print(f'{date_route_pointer.route} from {from_date} to {to_date}')
            # criteria = {"$and": [{"RecordedAtTimeDatetime": {"$gte": from_date, "$lte": to_date}},{ "MonitoredVehicleJourney.LineRef" : lineref } ]}

            from_date = date_route_pointer.timestamp.isoformat()
            to_date = (date_route_pointer.timestamp + timedelta(hours = 1)).isoformat()
            print(f'{date_route_pointer.route} from {from_date} to {to_date}')
            criteria = {"$and": [{"RecordedAtTime": {"$gte": from_date, "$lte": to_date}},{ "MonitoredVehicleJourney.LineRef" : lineref } ]}
            print(criteria)



            payload = json.loads(dumps(Collection.find(criteria)))

            # make a json out of it
            response = { "scope": "history",
                         "query": {
                             "lineref": lineref
                         },
                         "payload": payload
                         }


            return json_util.dumps(response, indent=4)


#-------------- Pretty JSON -------------------------------------------------------------
# https://gitter.im/tiangolo/fastapi?at=5d381c558fe53b671dc9aa80
import json
import typing
from starlette.responses import Response
import logging

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



class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

class DatePointer():

    def __init__(self,timestamp):
        self.timestamp = timestamp
        self.year = timestamp.year
        self.month = timestamp.month
        self.day = timestamp.day
        self.hour = timestamp.hour
        self.purepath = self.get_purepath()

    def get_purepath(self):
        return PurePath(str(self.year),
                        str(self.month),
                        str(self.day),
                        str(self.hour)
                        )

    def __repr__(self):
        return ('-'.join([str(self.year),
                          str(self.month),
                          str(self.day),
                          str(self.hour)
                          ]))

class DateRoutePointer(DatePointer):

    def __init__(self,timestamp,route=None):
        super().__init__(timestamp)
        self.route = route
        self.purepath = PurePath(self.purepath, self.route) #extend self.purepath inherited from parent.super()

    def __repr__(self):
        return ('-'.join([str(self.year),
                          str(self.month),
                          str(self.day),
                          str(self.hour),
                          self.route]))
