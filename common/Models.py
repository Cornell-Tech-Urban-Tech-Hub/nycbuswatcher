import os
import json
import pickle
import tarfile
from dateutil import parser
import inspect
import logging
from collections import defaultdict
from decimal import Decimal
from shutil import copy

from datetime import date, datetime, timedelta
from pathlib import Path, PurePath
from glob import glob
from uuid import uuid4

from pymongo import MongoClient
from bson import json_util
from bson.json_util import dumps

from common.Helpers import timer, delete_keys_from_dict

import common.config.config as config

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

# with help from https://realpython.com/introduction-to-mongodb-and-python/
class MongoLake():

    def __init__(self, python_env, localhost_mode):
        self.uid = uuid4().hex
        if python_env == "production" :
            self.db_host = "db"
        if python_env == "development" or localhost_mode == True:
            self.db_host = "localhost"

    def get_mongo_client(self):
        return MongoClient(host=self.db_host, port=27017)

    def store_feeds(self, feeds):
        with self.get_mongo_client() as client:
            db = client.nycbuswatcher # db name is 'buswatcher'
            response_db = db["siri_archive"] # raw responses
            buses_db = db["buses"] # only the ['MonitoredVehicleJourney'] dicts

            # iterate over each route
            for route_report in feeds:
                for route_id, response in route_report.items():

                    # dump the response to archive
                    response_db.insert_one(response.json())

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



