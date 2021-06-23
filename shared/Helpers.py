import os
import datetime as dt
import requests
import pickle

from functools import wraps
from time import time

def timeit(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time() * 1000)) - start
            print(f"Total execution time: {end_ if end_ > 0 else 0} ms")
    return _time_it

def get_OBA_routelist():
    url = "http://bustime.mta.info/api/where/routes-for-agency/MTA%20NYCT.json?key=" + os.getenv("API_KEY")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 503: # response is bad, so go to exception and load the pickle
            raise Exception(503, "503 error code fetching route definitions. OneBusAway API probably overloaded.")
        else: # response is good, so save it to pickle and proceed
            with open(('../data/routes-for-agency.pickle'), "wb") as pickle_file:
                pickle.dump(response,pickle_file)
    except Exception as e: # response is bad, so load the last good pickle
        with open(('../data/routes-for-agency.pickle'), "rb") as pickle_file:
            response = pickle.load(pickle_file)
        print("Route URLs loaded from pickle cache.")
    finally:
        routes = response.json()
        now=dt.datetime.now()
        # print('Found {} routes at {}.'.format(len(routes['data']['list']),now.strftime("%Y-%m-%d %H:%M:%S")))

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

