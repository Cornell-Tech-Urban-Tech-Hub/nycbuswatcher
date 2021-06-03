import argparse
import os
import time

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

import shared.Dumpers as dump


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
        scheduler.add_job(dump.async_grab_and_store(args.localhost), 'interval', seconds=interval, max_instances=2, misfire_grace_time=15)

        #todo activate hourly jobs
        # every hour
        scheduler.add_job(dump.DataLake.render_puddles(), 'interval', minutes=60, max_instances=1, misfire_grace_time=15) # bundle up pickles and write static file for API
        # scheduler.add_job(dump.DataStore.render_barrels(), 'interval', minutes=60, max_instances=1, misfire_grace_time=15) # bundle up pickles and write static file for API

        scheduler.start()

        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    # DEVELOPMENT = run once and quit
    elif os.environ['PYTHON_ENV'] == "development":
        dump.async_grab_and_store(args.localhost)



