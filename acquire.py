import argparse
import os
import time
import datetime as dt

from apscheduler.schedulers.background import BackgroundScheduler
import trio
from dotenv import load_dotenv

import shared.Helpers as help
import shared.Dumpers as dump
import shared.GTFS2GeoJSON as GTFS2GeoJSON

from shared.config import config


def async_grab_and_store():

    start = time.time()
    SIRI_request_urlpaths = help.get_SIRI_request_urlpaths()
    feeds = []

    async def grabber(s,a_path,route_id):
        try:
            r = await s.get(path=a_path)
        except Exception as e :
            print ('{} from DNS issues'.format(e))
        feeds.append({route_id:r})

    async def main(path_list):
        from asks.sessions import Session

        if args.localhost is True:
            s = Session('http://bustime.mta.info', connections=5)
        else:
            s = Session('http://bustime.mta.info', connections=config.config['http_connections'])
        async with trio.open_nursery() as n:
            for path_bundle in path_list:
                for route_id,path in path_bundle.items():
                    n.start_soon(grabber, s, path, route_id )

    trio.run(main, SIRI_request_urlpaths)

    # dump to the various locations
    timestamp = dt.datetime.now().strftime("%Y-%m-%dT_%H:%M:%S.%f")
    dump.to_barrel(feeds, timestamp) # parse and pickle all the visible buses as BusObservation objects
    dump.to_files(feeds, timestamp) # save the original JSON responses into files
    dump.to_lastknownpositions(feeds) # make a GeoJSON file for real-time map

    # report results to console
    num_buses = help.num_buses(feeds)
    end = time.time()
    print('Fetched {} BusObservations on {} routes in {:2f} seconds to pickle barrel and responsepath.\n'.format(num_buses,len(feeds),(end - start)))
    return


if __name__ == "__main__":

    print('NYC MTA BusTime API Scraper v2.0 (no-database branch) June 2021. Anthony Townsend <atownsend@cornell.edu>')
    print('mode: {}'.format(os.environ['PYTHON_ENV']))

    parser = argparse.ArgumentParser(description='NYCbuswatcher grabber, fetches and stores current position for buses')
    parser.add_argument('-l', action="store_true", dest="localhost", help="force localhost for production mode")
    args = parser.parse_args()

    load_dotenv()

    # PRODUCTION = start main loop
    if os.environ['PYTHON_ENV'] == "production":
        interval = 60
        print('Scanning on {}-second interval.'.format(interval))
        scheduler = BackgroundScheduler()

        # every minute
        scheduler.add_job(async_grab_and_store, 'interval', seconds=interval, max_instances=2, misfire_grace_time=15)

        # every hour
        # scheduler.add_job(dump.render_barrel, 'interval', minutes=60, max_instances=1, misfire_grace_time=15) # bundle up pickles and write static file for API
        # scheduler.add_job(dump.tarball_responses, 'interval', minutes=60, max_instances=1, misfire_grace_time=15) # bundle up pickles and write static file for API

        # every day
        # scheduler.add_job(GTFS2GeoJSON.update_route_map, 'cron', hour='2') # rebuilds the system map file, run at 2am daily
        # scheduler.add_job(dump.rotate_files,'cron', hour='1') #run at 1 am daily

        scheduler.start()

        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    # DEVELOPMENT = run once and quit
    elif os.environ['PYTHON_ENV'] == "development":
        async_grab_and_store()



