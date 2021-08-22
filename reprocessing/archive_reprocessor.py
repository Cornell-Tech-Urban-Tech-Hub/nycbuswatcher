# archive-reprocessor.py
# 30 april 2021

import argparse
import gzip
import shutil
import ijson
from common.Database_reprocessor import *


def db_init(dest, daily_filename):
    daily_filename=daily_filename[:-3] #remove .gz
    db_url = get_db_url(dest, daily_filename)
    create_table(db_url)
    session = get_session(db_url)
    return session

# after https://www.aylakhan.tech/?p=27
def extract_responses(f):
    responses = ijson.items(f, 'Siri', multiple_values=True)
    for response in responses:
        yield response


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='NYCbuswatcher reprocessor, fetches and stores current position for buses')
    parser.add_argument('datadir', type=str, help="Path to daily response file archives.")
    parser.add_argument('-d', '--dest', nargs=1, required=True, choices=['sqlite', 'mysql'], help="Destination (valid types are: sqlite, mysql, csv)")
    args = parser.parse_args()


    datadir = args.datadir+'/'
    dailies = get_daily_filelist(datadir)

    for daily_filename in dailies:
        time_started = datetime.datetime.now()
        print('started at {}'.format(datetime.datetime.now()))

        gzipfile = datadir + daily_filename
        jsonfile = datadir + '{}.json'.format(daily_filename[:-3])

        # try to load the uncompressed file from disk
        try:
            f = open(jsonfile, 'r')
            f.close()

        # if not exist, unzip it
        except:
            print('Unzipping {}{}'.format(datadir, daily_filename))
            with gzip.open(gzipfile, 'rb') as f_in:
                with open(jsonfile, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        finally:
            # parse and dump all responses
            # https://pypi.org/project/json-stream-parser/
            print('Parsing JSON responses and dumping to db.')
            with open(jsonfile, 'r') as f:
                session = db_init(args.dest[0], daily_filename)

                # THIS WORKS
                for siri_response in extract_responses(f):
                    buses = parse_response(siri_response)
                    session.bulk_save_objects(buses)

                    # commit after each pass
                    if args.dest[0] == 'mysql':
                        session.commit()

        # ok here for testing on sqlite, but not mysql
        # session.commit()


        #remove the json file
        try:
            os.path.exists(jsonfile)
            os.remove(jsonfile)
        except:
            pass

        # close
        time_finished = datetime.datetime.now()
        print ('finished at {}'.format(time_finished))
        print ('time elapsed: {}'.format(time_finished-time_started))


