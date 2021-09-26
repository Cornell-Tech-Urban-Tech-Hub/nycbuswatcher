import requests
from time import time
import datetime as dt
import trio

from common.Models import *

def async_grab_and_store(localhost, cwd):
    start = time()
    SIRI_request_urlpaths = get_SIRI_request_urlpaths()
    feeds = []

    async def grabber(s,a_path,route_id):
        try:
            r = await s.get(path=a_path, retries=2, timeout=10)
            feeds.append({route_id:r})
        except Exception as e:
            logging.error ('\tCould not fetch feed for {}. (Increase max retries for Session.get()?)'.format(route_id) )

    async def main(path_list):
        from asks.sessions import Session

        if localhost is True:
            s = Session('http://bustime.mta.info', connections=5)
        else:
            s = Session('http://bustime.mta.info', connections=config.config['http_connections'])
        async with trio.open_nursery() as n:
            for path_bundle in path_list:
                for route_id,path in path_bundle.items():
                    n.start_soon(grabber, s, path, route_id )

    trio.run(main, SIRI_request_urlpaths)

    # dump to the various locations
    timestamp = dt.datetime.now()
    DataLake(cwd).make_puddles(feeds, DatePointer(timestamp))
    DataStore(cwd).make_barrels(feeds, DatePointer(timestamp))

    # report results to console
    n_buses = num_buses(feeds)
    end = time()
    logging.debug('Saved {} route feeds into puddles and pickled {} BusObservations in {:2f} seconds at {}.'.format(len(feeds),n_buses,(end - start), dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return


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
