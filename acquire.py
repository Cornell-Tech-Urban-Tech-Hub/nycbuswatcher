import argparse
import time
import logging
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from common.Models import *
from common.Grabber import async_grab_and_store

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='NYCbuswatcher grabber, fetches and stores current position for buses')
    parser.add_argument('-l', action="store_true", dest="localhost", help="force localhost for production mode")
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")

    args = parser.parse_args()
    print('NYC MTA BusTime API Scraper v2.0 (no-database branch) June 2021. Anthony Townsend <atownsend@cornell.edu>')

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
        print ('VERBOSE ON')
    else:
        logging.basicConfig(level=logging.WARNING)
        print ('VERBOSE OFF, WARNINGS+ERRORS ONLY')

    load_dotenv()

    # PRODUCTION = start main loop
    if os.environ['PYTHON_ENV'] == "production":

        scheduler = BackgroundScheduler()

        # every minute
        scan_interval_seconds = 60

        logging.debug('{} mode. Scanning on {}-second interval.'.format(os.environ['PYTHON_ENV'].capitalize(), scan_interval_seconds))
        scheduler.add_job(async_grab_and_store,
                          'interval',
                          args=[args.localhost, Path.cwd()],
                          seconds=scan_interval_seconds,
                          misfire_grace_time=15)


        lake = DataLake(Path.cwd()) # wont need to implement a load_lake method because this doesn't suffer from scaling like the DataStore does.
        store = DataStore(Path.cwd())

        # # every 15 minutes
        # scheduler.add_job(store.dump_dashboard,
        #                   'interval',
        #                   minutes=5,
        #                   misfire_grace_time=60)

        # every hour, 2 minutes after the hour
        scheduler.add_job(lake.freeze_puddles,
                      'cron',
                      hour='*',
                      minute=2,
                      misfire_grace_time=300)
        scheduler.add_job(store.render_barrels,
                          'cron',
                          hour='*',
                          minute=2,
                          misfire_grace_time=300)

        # every hour, 15 minutes after the hour
        scheduler.add_job(store.make_shipment_indexes,
                          'cron',
                          hour='*',
                          minute=15,
                          misfire_grace_time=300)

        # every hour, 45 minutes after the hour
        scheduler.add_job(lake.make_glacier_indexes,
                          'cron',
                          hour='*',
                          minute=45,
                          misfire_grace_time=300)

        '''
        # every night, at 3 am
        scheduler.add_job(make_route_histories(),
                          'cron',
                          hour='3',
                          minute=0,
                          misfire_grace_time=300)
        '''



        # Start the schedulers
        scheduler.start()

        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    # DEVELOPMENT = run once and quit
    elif os.environ['PYTHON_ENV'] == "development":
        async_grab_and_store(args.localhost, Path.cwd())



