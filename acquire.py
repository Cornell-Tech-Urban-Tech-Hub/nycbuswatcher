import argparse
import os
import time
import datetime as dt
import trio
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

import shared.Dumpers as data
from shared.config import config
import shared.Helpers as help

def async_grab_and_store():
    start = time.time()
    SIRI_request_urlpaths = help.get_SIRI_request_urlpaths()
    feeds = []

    async def grabber(s,a_path,route_id):
        try:
            r = await s.get(path=a_path)
            feeds.append({route_id:r}) # UnboundLocalError: local variable 'r' referenced before assignment
        #bug find a way to retry these, connection errors lead to a gap for any route that raises an Exception
        except Exception as e:
            print (route_id, e)

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
    timestamp = dt.datetime.now()
    # date_pointer = timestamp.replace(microsecond=0, second=0, minute=0)
    data.DataLake().make_puddles(feeds,timestamp)
    # data.DataStore(date_pointer).make_barrels(feeds,timestamp)

    # report results to console
    num_buses = help.num_buses(feeds)
    end = time.time()
    print('Fetched and saved {} route feeds and pickled {} BusObservations in {:2f} seconds at {}.\n'.format(len(feeds),num_buses,(end - start), dt.datetime.now()))
    return

if __name__ == "__main__":

    print('NYC MTA BusTime API Scraper v2.0 (no-database branch) June 2021. Anthony Townsend <atownsend@cornell.edu>')
    print('mode: {}'.format(os.environ['PYTHON_ENV']))

    parser = argparse.ArgumentParser(description='NYCbuswatcher grabber, fetches and stores current position for buses')
    parser.add_argument('-l', action="store_true", dest="localhost", help="force localhost for production mode")
    parser.add_argument('--dry-run', action="store_true", dest="dry-run", help="Force dry run (dont write or delete anything) not implemented")
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
        scheduler.add_job(dump.DataLake(args).archive_puddles, 'interval', minutes=60, max_instances=1, misfire_grace_time=15) # bundle up pickles and write static file for API
        # scheduler.add_job(dump.DataStore.render_barrels(), 'interval', minutes=60, max_instances=1, misfire_grace_time=15) # bundle up pickles and write static file for API

        scheduler.start()

        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    # DEVELOPMENT = run once and quit
    elif os.environ['PYTHON_ENV'] == "development":
        async_grab_and_store()



