import argparse
import time
import datetime as dt
import trio
from apscheduler.schedulers.background import BackgroundScheduler

from dotenv import load_dotenv

from shared.Models import *
import shared.config.config as config
import shared.Helpers as help

def async_grab_and_store():
    start = time.time()
    SIRI_request_urlpaths = help.get_SIRI_request_urlpaths()
    feeds = []

    async def grabber(s,a_path,route_id):
        try:
            r = await s.get(path=a_path)
            #todo HIGH find a way to retry these, connection errors lead to a gap for any route that raises an Exception
            feeds.append({route_id:r}) # UnboundLocalError: local variable 'r' referenced before assignment
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
    DataLake().make_puddles(feeds, DatePointer(timestamp))
    DataStore().make_barrels(feeds, DatePointer(timestamp))

    # report results to console
    num_buses = help.num_buses(feeds)
    end = time.time()
    print('Fetched and saved {} route feeds and pickled {} BusObservations in {:2f} seconds at {}.'.format(len(feeds),num_buses,(end - start), dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return

if __name__ == "__main__":

    print('NYC MTA BusTime API Scraper v2.0 (no-database branch) June 2021. Anthony Townsend <atownsend@cornell.edu>')

    parser = argparse.ArgumentParser(description='NYCbuswatcher grabber, fetches and stores current position for buses')
    parser.add_argument('-l', action="store_true", dest="localhost", help="force localhost for production mode")
    args = parser.parse_args()

    load_dotenv()

    # PRODUCTION = start main loop
    if os.environ['PYTHON_ENV'] == "production":

        scheduler = BackgroundScheduler()

        # every minute
        scan_interval_seconds = 60
        print('{} mode. Scanning on {}-second interval.'.format(os.environ['PYTHON_ENV'].capitalize(), scan_interval_seconds))
        scheduler.add_job(async_grab_and_store, 'interval', seconds=scan_interval_seconds, max_instances=2, misfire_grace_time=15)

        # # every 15 minutes
        lake = DataLake()
        store = DataStore()
        scheduler.add_job(store.dump_dashboard, 'interval', minutes=5, misfire_grace_time=1)

        # # every hour
        scheduler.add_job(lake.freeze_puddles, 'interval', minutes=60,  misfire_grace_time=5)
        scheduler.add_job(store.render_barrels, 'interval', minutes=60,  misfire_grace_time=5)

        # todo add an hourly job to update the dashboard.csv file that's read by dashboard.py

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



