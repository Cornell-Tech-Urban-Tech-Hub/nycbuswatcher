import argparse, pathlib
from common.Models import *
import ijson, gzip, shutil
from fnmatch import fnmatch
import datetime
from pathlib import PurePath

# archived response data should be in the ./reprocessor/input folder
# that folder is ignored by docker and git
# results of this process will output to

#################################################################################

cwd = Path.cwd()
topdir=PurePath('reprocessor')
input_path=topdir / 'input'
output_path=topdir / 'output'
lake = DataLake(output_path)
store = DataStore(output_path)

#################################################################################

# after https://www.aylakhan.tech/?p=27
def extract_responses(f):
    responses = ijson.items(f, 'Siri', multiple_values=True)
    for response in responses:
        yield response

def get_daily_filelist(path):
    daily_filelist=[]
    include_list = ['daily*.gz']
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            if any(fnmatch(filename, pattern) for pattern in include_list):
                daily_filelist.append(filename)
    sorted_daily_filelist = sorted(daily_filelist, key=lambda daily_filelist: daily_filelist[6:16])
    return sorted_daily_filelist

#################################################################################



if __name__=="__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description='NYCbuswatcher shipment dumper, dumps from monthly SIRI response archives to shipments')
    # parser.add_argument('-m','--months', nargs='+', help='<Required> List of months to process (leading zero, no year, e.g. -m 10 11 12 01 = Oct 2020, Nov 2020, Dec 2020, and Jan 2021)', required=True)
    parser.add_argument('filename')

    args = parser.parse_args()

    # 1 make sure ./reprocessor exists
    pathlib.Path(topdir).mkdir(parents=True, exist_ok=True)

    # 2 unpack the archive into a temp file
    daily_filename_list = get_daily_filelist(input_path)

    # 3 parse each response file
    for daily_filename in daily_filename_list:
        time_started = datetime.datetime.now()
        print('started at {}'.format(datetime.datetime.now()))

        gzipfile = input_path / daily_filename
        jsonfile = input_path / f'{daily_filename[:-3]}.json'

        # try to load the uncompressed JSON file from disk
        try:
            f = open(jsonfile, 'r')
            f.close()

        # if not exist, unzip it
        except:
            print(f'Unzipping {input_path}/{daily_filename}')
            with gzip.open(gzipfile, 'rb') as f_in:
                with open(jsonfile, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        finally:

            print('Parsing JSON responses and dumping to db.')

            # inspect each siri response
            timestamp = datetime.datetime.now()

            # open the day's JSON responses
            with open(jsonfile, 'r') as f:

                # separate the responses
                for siri_response in extract_responses(f):

                    # put each response in the DataStore
                    store.make_barrels(siri_response, DatePointer(timestamp))

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


        #todo call store.render_shipments()

