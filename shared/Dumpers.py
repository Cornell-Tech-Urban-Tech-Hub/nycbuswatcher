import datetime
import json
import pickle
from pathlib import Path
from glob import glob
import geojson
import tarfile
import sys
import os
from shared.BusObservation import BusObservation

pathmap = {
    'lake': 'data/lake',
    'store':'data/store',
    'barrel':'data/store/barrels',
    'puddle':'data/lake/puddles'
}

#-------------------------------------------------------------------------------------------------------------------------------------
# Data Lake + Puddles
#-------------------------------------------------------------------------------------------------------------------------------------
# puddles are temporary storage folders for an hour's worth of JSON repsonses for a single route

# a DataLake instance represents all of the Puddles
class DataLake():

    def __init__(self,*runtime_args):
        self.puddles = self.load_puddles()
        self.path_check()
        self.runtime_args = runtime_args
        # todo other data late metadata -- array of dates and hours covered, total # of records, etc.

    # verify the lake exists — probably redundant
    def path_check(self):
        lake = Path.cwd() / pathmap['lake']
        lake.mkdir(parents=True, exist_ok=True)
        return

    def timestamp_to_date_pointer(self, timestamp):
        return datetime.datetime.now().replace(microsecond=0, second=0, minute=0)

    # dump each response to data/puddle/YYYY/MM/DD/HH/route_id/drop_2021-04-03T12:12:12.json
    def make_puddles(self, feeds, timestamp):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                try:

                    # prepare the puddle
                    folder = Path(Puddle(self.timestamp_to_date_pointer(timestamp), route).folder.path )
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

    # future factor this out of both DataLake and DataStore into a GenericStore parent class
    def date_pointer_from_a_path(self,apath):
        parts = apath.split('/')
        year, month, day, hour = int(parts[-5]), \
                                 int(parts[-4]), \
                                 int(parts[-3]), \
                                 int(parts[-2])
        # print (year, month, day, hour)
        date_pointer=datetime.datetime(year, month, day, hour)
        return date_pointer

    # builds a list of paths to puddles — e.g. hourly route folders of JSON files
    def load_puddles(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['puddle']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)

        puddles = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            route = d.split('/')[-1]
            puddles.append(Puddle(date_pointer, route))

        return puddles

    #TODO REWRITE THIS USING NEW CLASS STRUCTURE

# a Puddle is a folder holding raw JSON responses for a single route, single hour
class Puddle():

    def __init__(self, date_pointer, route):
        self.folder = GenericFolder(date_pointer, route, kind='puddle') #self.folder.path = the path of this puddle
        self.date_pointer = date_pointer
        self.route = route

    # TODO
   # def render_myself_to_tarball(self):
   #     unrendered_puddlepaths = self.gen_puddle_list()
   #
   #     puddles_to_render=[]
   #     for p in unrendered_puddlepaths:
   #         date_pointer=self.date_pointer_from_puddlepath(p)
   #         current_hour = datetime.datetime.now().replace(microsecond=0, second=0, minute=0)
   #         if date_pointer == current_hour:
   #             # print ('puddle EXCLUDED {}'.format(p))
   #             continue
   #         elif date_pointer != current_hour:
   #             puddles_to_render.append(Puddle(date_pointer))
   #
   #
   #     for p in puddles_to_render:
   #         print (p.path)
   #         # 1 make a list of all the files to include
   #         drops_to_render = glob('{}*.dat'.format(p))
   #         # print (drops_to_render)
   #
   #         # 2 create the outfile
   #         lake = DataLake().pa
   #         outfile = lake / str(p.date_pointer.year) / str(p.date_pointer.month) / str(p.date_pointer.day) / str(p.date_pointer.hour) / 'lake_{}.tar.gz'.format(outfile_stem, outfile_route)
   #
   #
   #         # 3 render the files to the outfile
   #
   #         with tarfile.open(outfile, "w:gz") as tar:
   #             for file in drops_to_render:
   #                 tar.add(file)
   #
   #
   #         ## create the outfile string/path
   #         outfile_route=Path(puddlepath).parts[-1] # grab last part of path
   #         outfile_stem=puddlepath.replace('puddles','lakes')
   #         outfile=
   #
   #         for file in puddles_to_render:
   #             try:
   #                 print ('removing {}'.format(file))
   #                 os.remove(file)
   #             except:
   #                 pass

#-------------------------------------------------------------------------------------------------------------------------------------
# Data Store + Barrels
#-------------------------------------------------------------------------------------------------------------------------------------
# barrels are temporary storage folders for an hour's worth of pickled BusObservations (in .dat files) for a single route

# a DataStore instance represents all of the Barrels
class DataStore():
    def __init__(self, *runtime_args):
        self.barrels = self.load_barrels()
        self.path_check()
        self.runtime_args = runtime_args
        # todo calculate some metadata about the whole data store and keep it here (array of dates and hours covered, # of records, etc.)

    # verify the store exists — probably redundant
    def path_check(self):
        store = Path.cwd() / pathmap['store']
        store.mkdir(parents=True, exist_ok=True)
        return

    def timestamp_to_date_pointer(self, timestamp):
        return datetime.datetime.now().replace(microsecond=0, second=0, minute=0)

    # dump each pickle to data/barrel/YYYY/MM/DD/HH/route_id/barrel_2021-04-03T12:12:12.dat
    def make_barrels(self,feeds,timestamp):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route = route_id.split('_')[1]
                pickles = []
                try:

                    # prepare the barrel
                    folder = Path(Barrel(self.timestamp_to_date_pointer(timestamp), route).folder.path )
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

    # future factor this out of both DataLake and DataStore into a GenericStore parent class
    def date_pointer_from_a_path(self,apath):
        parts = apath.split('/')
        year, month, day, hour = int(parts[-5]), \
                                 int(parts[-4]), \
                                 int(parts[-3]), \
                                 int(parts[-2])
        # print (year, month, day, hour)
        date_pointer=datetime.datetime(year, month, day, hour)
        return date_pointer

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
class Barrel():

    def __init__(self, date_pointer, route):
        self.folder = GenericFolder(date_pointer, route, kind='barrel') #self.folder.path = the path of this barrel
        self.date_pointer = date_pointer
        self.route = route

    # TODO

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

class GenericFolder():

    def __init__(self, date_pointer, route, kind = None):
        self.path = self.get_path(pathmap[kind], date_pointer, route)
        self.checkpath(self.path)

    # generate the unique path for this folder
    def get_path(self, prefix, date_pointer, route):
        folderpath = Path.cwd() / prefix / str(date_pointer.year) / str(date_pointer.month) / str(date_pointer.day) / str(date_pointer.hour) / route
        self.checkpath(folderpath)
        return folderpath

    # create folder if it doesn't exist, including parents
    def checkpath(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return
