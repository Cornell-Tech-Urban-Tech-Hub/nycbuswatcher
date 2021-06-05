import datetime
import json
import pickle
from pathlib import Path
from glob import glob
import geojson
import tarfile
import sys
import os

import shared
from shared.BusObservation import BusObservation

pathmap = {
    'lake':
        {'root':'data/lake'},
    'puddle':
        {'root':'data/lake/puddles',
         'archive':'data/lake/archive'},
    'store':
        {'root':'data/store'},
    'barrel':
        {'root':'data/store/barrels',
         'archive':'data/store/static'}
}

#-------------------------------------------------------------------------------------------------------------------------------------
# base Classes
#-------------------------------------------------------------------------------------------------------------------------------------
#

# future a bit of these two is redundant
class GenericStore():

    def __init__(self, kind=None):
        self.path = self.get_path(pathmap[kind]['root'])
        self.checkpath(self.path)

    # generate the unique path for this folder
    def get_path(self, prefix):
        folderpath = Path.cwd() / prefix
        self.checkpath(folderpath)
        return folderpath

    # create folder if it doesn't exist, including parents
    def checkpath(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return

    # round a timestamp down to the hour to use as a date_pointer
    def timestamp_to_date_pointer(self, timestamp):
        return datetime.datetime.now().replace(microsecond=0, second=0, minute=0)

    # extract a date_pointer from a full route path e.g ..../2021/02/23/11/Bx4
    def date_pointer_from_a_path(self,apath):
        parts = apath.split('/')
        year, month, day, hour = int(parts[-5]), \
                                 int(parts[-4]), \
                                 int(parts[-3]), \
                                 int(parts[-2])
        # print (year, month, day, hour)
        date_pointer=datetime.datetime(year, month, day, hour)
        return date_pointer


class GenericFolder():

    def __init__(self, date_pointer, route, kind = None):
        self.path = self.get_path(pathmap[kind]['root'], date_pointer, route)
        self.checkpath(self.path)
        if kind in ['puddle', 'barrel']:
            self.archive_path = self.get_path(pathmap[kind]['archive'], date_pointer, route)
            self.checkpath(self.archive_path)

    # generate the unique path for this folder
    def get_path(self, prefix, date_pointer, route):
        folderpath = Path.cwd() / prefix / str(date_pointer.year) / str(date_pointer.month) / str(date_pointer.day) / str(date_pointer.hour) / route
        self.checkpath(folderpath)
        return folderpath

    # create folder if it doesn't exist, including parents
    def checkpath(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return

    def delete_folder(self):
        # https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
        def rm_tree(pth):
            pth = Path(pth)
            for child in pth.glob('*'):
                if child.is_file():
                    child.unlink()
                else:
                    rm_tree(child)
            pth.rmdir()

        rm_tree(self.path)
        return

#-------------------------------------------------------------------------------------------------------------------------------------
# Data Lake + Puddles
#-------------------------------------------------------------------------------------------------------------------------------------
# puddles are temporary storage folders for an hour's worth of JSON responses for a single route

# a DataLake instance represents all of the Puddles
class DataLake(GenericStore):

    def __init__(self, *runtime_args):
        super().__init__(kind='lake')
        self.puddles = self.load_puddles()
        self.runtime_args = runtime_args
        # todo other metadata -- array of dates and hours covered, total # of records, etc.

    # dump each response to data/puddle/YYYY/MM/DD/HH/route_id/drop_2021-04-03T12:12:12.json
    def make_puddles(self, feeds, timestamp):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                try:

                    # prepare the puddle
                    folder = Path(Puddle(self.timestamp_to_date_pointer(timestamp), route).path)
                    filename = 'drop_{}_{}.json'.format(route,timestamp).replace(' ', 'T')
                    filepath = folder / filename

                    # parse the response
                    route_data = route_data.json()

                    # write it
                    with open(filepath, 'wt', encoding="ascii") as f:
                        json.dump(route_data, f, indent=4)

                except Exception as e: # no vehicle activity?
                    print (e)
                    pass
        return

    # builds a list of paths to puddles — e.g. hourly route folders of JSON files
    def load_puddles(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['puddle']['root']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        puddles = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            route = d.split('/')[-1]
            puddles.append(Puddle(date_pointer, route))
        return puddles

    # creates a list of all puddles that are not in current hour and can be archived
    def list_expired_puddles(self):
        expired_puddles = []
        for puddle in self.puddles:
            now_date_pointer = self.timestamp_to_date_pointer(datetime.datetime.now())
            if puddle.date_pointer != now_date_pointer:
                expired_puddles.append(puddle)
        return expired_puddles

    # tar up all the files in all expired puddles (not the current hour)
    def archive_puddles(self):
        puddles_to_archive = self.list_expired_puddles()
        for puddle in puddles_to_archive:
            puddle.render_myself_to_tarball()
        return

# a Puddle is a folder holding raw JSON responses for a single route, single hour
class Puddle(GenericFolder):

    def __init__(self, date_pointer, route):
        super().__init__(date_pointer, route, kind='puddle')
        self.date_pointer = date_pointer
        self.route = route

    #bug verify tarball contents and deletion is limited to expired hours
    def render_myself_to_tarball(self):

        # round up all the files in me and tar them to ??
        drops_to_archive=[x for x in glob("{}*.json".format(self.path)) if x.is_file()]

        outfile = self.archive_path / 'lake_{}.tar.gz'.format(self.date_pointer.year,
                                          self.date_pointer.month,
                                          self.date_pointer.day,
                                          self.date_pointer.hour,
                                          self.route
                                          )
        with tarfile.open(outfile, "w:gz") as tar:
                for drop in drops_to_archive:
                    tar.add(drop)

        print ('made a tarball called {} out of {}'.format(outfile,[x for x in drops_to_archive]))

        self.delete_folder()
        # print ('and deleted the puddle at {}'.format(self.path)

        return



#-------------------------------------------------------------------------------------------------------------------------------------
# Data Store + Barrels
#-------------------------------------------------------------------------------------------------------------------------------------
# barrels are temporary storage folders for an hour's worth of pickled BusObservations (in .dat files) for a single route

# a DataStore instance represents all of the Barrels
class DataStore(GenericStore):

    def __init__(self, *runtime_args):
        super().__init__(kind='store')
        self.barrels = self.load_barrels()
        self.runtime_args = runtime_args
        # todo calculate some metadata about the whole data store and keep it here (array of dates and hours covered, # of records, etc.)


    # dump each pickle to data/barrel/YYYY/MM/DD/HH/route_id/barrel_2021-04-03T12:12:12.dat
    def make_barrels(self,feeds,timestamp):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route = route_id.split('_')[1]
                pickles = []
                try:

                    # prepare the barrel
                    folder = Path(Barrel(self.timestamp_to_date_pointer(timestamp), route).path )
                    filename = 'pickle_{}_{}.dat'.format(route,timestamp).replace(' ','T')
                    filepath = folder / filename

                    # parse the response
                    route_data = route_data.json()
                    for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                        bus = BusObservation(route, monitored_vehicle_journey)
                        pickles.append(bus)

                    # write it
                    with open(filepath, "wb") as f:
                        pickle.dump(pickles, f)

                except KeyError:
                    # future find a way to filter these out to reduce overhead
                    # this is almost always the result of a route that doesn't exist, so why is it in the OBA response?
                    # 'VehicleActivity' M79+ {'Siri': {'ServiceDelivery': {'ResponseTimestamp': '2021-06-04T07:38:01.071-04:00', 'VehicleMonitoringDelivery': [{'ResponseTimestamp': '2021-06-04T07:38:01.071-04:00', 'ErrorCondition': {'OtherError': {'ErrorText': 'No such route: MTA NYCT_M79 .'}, 'Description': 'No such route: MTA NYCT_M79 .'}}]}}}
                    pass
                except json.JSONDecodeError:
                    # this is probably no response?
                    print ('JSONDecodeError: No/Bad API response? - route {}'.format(route))
                    pass
        return


    # future this could be abstracted into GenericStore by passing kind in and using generic names?
    # builds a list of paths to barrels — e.g. hourly route folders of dat files
    def load_barrels(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['barrel']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)

        barrels = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            route = d.split('/')[-1]
            barrels.append(Barrel(date_pointer, route))

        return barrels


    #TODO
    #todo logic to handle duplicates and missings should be in the barrel (e.g. it should manage itself)

    # # trigger each barrel to render itself to static
    # def render_barrels(self):
    #     for barrel in self.barrels:
    #         barrel.render_pickles_to_static()
    #         print('All old Barrels are empty.')
    #         return

# a Barrel is a folder holding files with multple pickled BusObservation instances parsed from responses for a single route, single hour
class Barrel(GenericFolder):

    def __init__(self, date_pointer, route):
        super().__init__(date_pointer, route, kind='barrel') #self.path = the path of this barrel
        self.date_pointer = date_pointer
        self.route = route

    # TODO port render code from Puddle class
    # def render_pickles_to_static(self):
    #
    #     def render_barrel(self):
    #
    #         # #  make sure we are not rendering the current hour barrel, which is still in use
    #         # more complete = compare year/month/date/hour
    #         # self.path.split('/')[-1] # compare the last item in the path (the hour) to the current hour
    #         current_datepath = '{}/{}/{}/{}'(datetime.datetime.now().year,
    #                                   datetime.datetime.now().month,
    #                                   datetime.datetime.now().day,
    #                                   datetime.datetime.now().hour)
    #         barrel_datepath = self.path.split('/')[2:].join('/')
    #         if current_datepath == barrel_datepath: # compare self.path starting with the 3rd element
    #             print ('stopping: i wasnt able to exclude the current hour folder. add some logic to avoid this, or run the grabber now to trigger it')
    #             sys.exit()
    #             continue # get out of here
    #         else:
    #             # if ok, continue to render the barrel
    #             picklefiles=[x for x in glob("*.dat") if x.is_file()] # checkme
    #
    #             pickle_array = []
    #             for picklefile in picklefiles:
    #                 # print('checking {}'.format(picklefile))
    #                 with open(picklefile, 'rb') as pickle_file:
    #                     barrel = pickle.load(pickle_file)
    #                     for p in barrel:
    #                         pickle_array.append(p)
    #                     # print ('added {} to route pickle'.format(picklefile))
    #
    #             #make a template for valid JSON
    #             json_container = dict()
    #
    #             #iterate over pickle_array and insert into JSON
    #             serial_array=[]
    #             for p in pickle_array:
    #                 serial_array.append(p.to_serial())
    #
    #             json_container['buses'] = serial_array
    #
    #
    #             static_path=route_folder.replace('data/barrel','static')+'/'
    #             checkpath(static_path)
    #
    #             rendered_file='rendered_barrel_{}.json'.format(route) # add date to filename?
    #             with open(static_path+'/'+rendered_file, 'w') as f:
    #                 json.dump(json_container, f)
    #
    #             print ('rendered {} pickles from {} files in {} barrel and dumped to static file {}'.format(len(pickle_array), len (picklefile_list), route, static_path+rendered_file))
    #
    #             print ('pickled {}'.format([p for p in picklefiles]))
    #
    #
    #     def render_pickles(self,data):
    #         get_pickles()
    #         j = jsonify
    #         with open(self.file_name, ‘w’) as f
    #         json.dump(j,f)
    #         return
    #
    #
    #     #     # debugged to here-------------------------------------------------------------
    #
    #     #         # figure out how to stop it from reprocessing old files
    #     #         #option 1=delete them
    #     #         #option 2=check if there is a rendered_barrel_{route}.json in static/2021/6/1/22/Q4/ already and if so, dont render
    #     #
    #     #         #remove everything in the barrel directory (but what if something is writing to it during the run?)
    #     #         #     for file in yesterday_gz_files:
    #     #         #         try:
    #     #         #             os.remove(file)
    #     #         #         except:
    #     #         #             pass
    #     #     return


#-------------------------------------------------------------------------------------------------------------------------------------
# Misc
#-------------------------------------------------------------------------------------------------------------------------------------

