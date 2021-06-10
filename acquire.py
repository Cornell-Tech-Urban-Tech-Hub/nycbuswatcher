import argparse
import os
import time
import datetime as dt
import trio
from apscheduler.schedulers.background import BackgroundScheduler

from dotenv import load_dotenv

import shared.DataStructures as data
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
        #bug LOW find a way to retry these, connection errors lead to a gap for any route that raises an Exception
        except Exception as e:
            print ('\tCould not fetch feed for {}. (Maybe you should write some retry code?)'.format(route_id) )

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
    data.DataLake().make_puddles(feeds, data.DatePointer(timestamp))
    data.DataStore().make_barrels(feeds, data.DatePointer(timestamp))

    # report results to console
    num_buses = help.num_buses(feeds)
    end = time.time()
    print('Fetched and saved {} route feeds and pickled {} BusObservations in {:2f} seconds at {}.'.format(len(feeds),num_buses,(end - start), dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return

if __name__ == "__main__":

    print('NYC MTA BusTime API Scraper v2.0 (no-database branch) June 2021. Anthony Townsend <atownsend@cornell.edu>')

    parser = argparse.ArgumentParser(description='NYCbuswatcher grabber, fetches and stores current position for buses')
    parser.add_argument('-l', action="store_true", dest="localhost", help="force localhost for production mode")
    parser.add_argument('--dry-run', action="store_true", dest="dry-run", help="Force dry run (dont write or delete anything) not implemented")
    args = parser.parse_args()

    load_dotenv()

    # PRODUCTION = start main loop
    if os.environ['PYTHON_ENV'] == "production":

        scheduler = BackgroundScheduler()

        # every minute
        interval = 60

        print('{} mode. Scanning on {}-second interval.'.format(os.environ['PYTHON_ENV'].capitalize(), interval))
        scheduler.add_job(async_grab_and_store, 'interval', seconds=interval, max_instances=2, misfire_grace_time=15)

        #bug lake.freeze_puddles doesn't work when run by APscheduler
        # but works fine if run direct in test.py


        #TODO try and use the decorator instead (in DataStructures.py )
        # https://apscheduler.readthedocs.io/en/stable/modules/triggers/cron.html?highlight=decorator
        # @sched.scheduled_job('interval', id='my_job_id', day='last sun')
        # then here, just instantiate the class and sleep?

        # # every hour
        lake = data.DataLake()
        store = data.DataStore()
        # scheduler.add_job(lake.freeze_puddles, 'cron', hour='*',  misfire_grace_time=15)
        # scheduler.add_job(store.render_barrels, 'cron', hour='*',  misfire_grace_time=15)

        scheduler.add_job(lake.freeze_puddles, 'interval', minutes=15,  misfire_grace_time=5)
        scheduler.add_job(store.render_barrels, 'interval', minutes=15,  misfire_grace_time=5)

        # scheduler.add_job(data.DataLake().freeze_puddles, 'cron', hour='*',  misfire_grace_time=15)
        # scheduler.add_job(data.DataStore().render_barrels, 'cron', hour='*',  misfire_grace_time=15)

        # Start the schedulers
        scheduler.start()

        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    # DEVELOPMENT = run once and quit
    elif os.environ['PYTHON_ENV'] == "development":
        async_grab_and_store()



