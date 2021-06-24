import argparse
import time

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from common.Models import *
from common.Grabber import async_grab_and_store

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
        scheduler.add_job(async_grab_and_store,
                          'interval',
                          args=[args.localhost, Path.cwd()],
                          seconds=scan_interval_seconds,
                          max_instances=2,
                          misfire_grace_time=15)

        # every 15 minutes
        lake = DataLake()
        store = DataStore()

        scheduler.add_job(store.dump_dashboard,
                          'interval',
                          minutes=5,
                          misfire_grace_time=1)

        # every hour
        scheduler.add_job(lake.freeze_puddles,
                          'interval',
                          minutes=60,
                          misfire_grace_time=5)

        scheduler.add_job(store.render_barrels,
                          'interval',
                          minutes=60,
                          misfire_grace_time=5)

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
        async_grab_and_store(args.localhost, Path.cwd())



