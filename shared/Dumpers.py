import os
import datetime
import json
import pickle
from pathlib import Path, PurePath
from glob import glob
import tarfile

from shared.BusObservation import BusObservation

pathmap = {
    'glacier':'data/lake/glaciers',
    'lake':'data/lake',
    'puddle':'data/lake/puddles',
    'shipment':'data/store/shipments',
    'store':'data/store',
    'barrel':'data/store/barrels'
    }

#-------------------------------------------------------------------------------------------------------------------------------------
# base Classes
#-------------------------------------------------------------------------------------------------------------------------------------
#

# these are methods and attributes shared by folders and files alike that have a date_pointer
# (though maybe that is not the right way to model?)

class DatePointer():

    def __init__(self,timestamp,route=None):
        self.timestamp = timestamp
        self.year = timestamp.year
        self.month = timestamp.month
        self.day = timestamp.day
        self.hour = timestamp.hour
        # https://stackoverflow.com/questions/15535655/optional-arguments-in-initializer-of-python-class
        self.route = route if route is not None else route
        self.purepath = self.get_purepath()

    def get_purepath(self):
        p = PurePath(str(self.year),
                     str(self.month),
                     str(self.day),
                     str(self.hour)
                     )
        if self.route is None:
            return p
        elif self.route is not None:
            return PurePath(p, PurePath(self.route))

    def __repr__(self):
        if self.route is None:
            return ('-'.join([str(self.year),
                              str(self.month),
                              str(self.day),
                              str(self.hour)
                              ]))
        elif self.route is not None:
            return ('-'.join([str(self.year),
                              str(self.month),
                              str(self.day),
                              str(self.hour),
                              self.route]))

# parent class for DataLake, DataStore
class GenericStore():

    def __init__(self, kind=None):
        super().__init__()
        self.path = self.get_path(pathmap[kind])
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

    # # round a timestamp down to the hour to use as a date_pointer
    # def timestamp_to_date_pointer(self, timestamp):
    #     return datetime.datetime.now().replace(microsecond=0, second=0, minute=0)

    # create a datepointer object by parsing a path
    def date_pointer_from_a_path(self, apath):
        parts = apath.split('/')
        return DatePointer(datetime.datetime(year=int(parts[-5]),
                                    month=int(parts[-4]),
                                    day=int(parts[-3]),
                                    hour=int(parts[-2])
                                    )
                           )

# parent class for Puddle, Barrel, ?Archive, ?Warehouse
class GenericFolder():

    def __init__(self, date_pointer, kind=None):
        super().__init__()
        self.path = self.get_path(pathmap[kind], date_pointer)
        self.checkpath(self.path)

    # generate the unique path for this folder
    def get_path(self, prefix, date_pointer):
        folderpath = Path.cwd() / prefix / date_pointer.purepath
        self.checkpath(folderpath)
        return folderpath

    # create folder if it doesn't exist, including parents
    def checkpath(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return
    # after https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    def delete_folder(self):
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
# Puddles in a DataLake are rendered into Glaciers
#-------------------------------------------------------------------------------------------------------------------------------------

# a DataLake instance represents all of the Puddles and Glaciers
class DataLake(GenericStore):

    def __init__(self):
        super().__init__(kind='lake')
        self.puddles = self.load_puddles()
        # future other metadata -- array of dates and hours covered, total # of records, etc.

    # dump each response to data/puddle/YYYY/MM/DD/HH/route_id/drop_2021-04-03T12:12:12.json
    def make_puddles(self, feeds, date_pointer):

        for route_report in feeds:

            for route_id,route_data in route_report.items():

                route=route_id.split('_')[1]
                puddle_date_pointer=DatePointer(date_pointer.timestamp,route)

                try:

                    # prepare the puddle
                    folder = Puddle(puddle_date_pointer).path
                    filename = 'drop_{}_{}.json'.format(puddle_date_pointer.route, puddle_date_pointer.timestamp).replace(' ', 'T')
                    filepath = folder / PurePath(filename)

                    # parse the response
                    route_data = route_data.json()

                    # write it
                    with open(filepath, 'wt', encoding="ascii") as f:
                        json.dump(route_data, f, indent=4)

                except Exception as e: # no vehicle activity?
                    # print (e)
                    # pass
                    import sys
                    sys.exit()
        return

    # builds a list of paths to puddles — e.g. hourly route folders of JSON files
    def load_puddles(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['puddle']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        puddles = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            date_pointer.route = d.split('/')[-1]
            puddles.append(Puddle(date_pointer))
        return puddles

    # creates a list of all puddles that are not in current hour and can be archived
    #bug sort list from oldest to newest, currently seems to work the other way
    def list_expired_puddles(self):
        expired_puddles = []
        for puddle in self.puddles:
            bottom_of_hour = DatePointer(datetime.datetime.now())
            bottom_of_hour.route=puddle.route
            if puddle.date_pointer != bottom_of_hour:
                expired_puddles.append(puddle)
        return expired_puddles

    # tar up all the files in all expired puddles (not the current hour)
    def freeze_puddles(self):
        print('firing DataLake.freeze_puddles')
        puddles_to_archive = self.list_expired_puddles()
        for puddle in puddles_to_archive:
            puddle.render_myself_to_archive()

        # quickly scan the whole datastore and delete empty folders
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(Path.cwd() / pathmap['lake']))):
            if not (files or dirnames):
                os.rmdir(dirpath)

        return

    # todo write DataLake.load_glaciers()
    def load_glaciers(self):
        return

# a Puddle is a folder holding raw JSON responses for a single route, single hour
class Puddle(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='puddle')
        self.date_pointer = date_pointer
        # fail to create if date_pointer.route doesn't exist
        if date_pointer.route is False:
            raise Exception ('tried to instantiate Puddle because you called __init__ without a value in DatePointer.route')
        self.route = date_pointer.route

    def render_myself_to_archive(self):
        print('firing Puddle.render_myself_to_archive')
        # round up all the files this puddle
        drops_to_archive=[x for x in self.path.glob('*.json') if x.is_file()]

        # init folder and sanity check -- is there already an archive file at this location?
        try:
            outfile = Glacier(self.date_pointer, self.route)
        except OSError:
            # future write handler for partially rendered puddles(maybe a different filename, or move it to a lost+found?)
            return

        # write the tarball
        with tarfile.open(outfile.filepath, "w:gz") as tar:
                for drop in drops_to_archive:
                    tar.add(drop, arcname=drop.name.replace(':','-')) # tar doesnt like colons
        print ('wrote {} drops to archive at {}'.format(len(drops_to_archive), outfile.path))

        # cleanup
        self.delete_folder()

        return

# an Glacier is a rendered Puddle, e.g. a tarball in a date_pointer/route folder
class Glacier(GenericFolder):

    def __init__(self, date_pointer, kind='glacier'):
        super().__init__(date_pointer, kind='glacier')
        self.date_pointer = date_pointer
        self.route = date_pointer.route
        self.exist, self.filepath = self.check_exist()
        if self.exist == True:
            raise OSError ('there is already an archive at {}'.format(self.filepath))


    def check_exist(self):
        filepath = self.path / 'glacier_{}_{}-{}-{}-{}.tar.gz'.format(self.date_pointer.route,
                                                                      self.date_pointer.year,
                                                                      self.date_pointer.month,
                                                                      self.date_pointer.day,
                                                                      self.date_pointer.hour
                                                                      )
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)

    # todo write Glacier.get_one
    # retrieves the appropriate file, optionally uncompresses it?
    # e.g. data = Glacier.thaw_one(DatePointer(datetime.datetime(year=2021, month=5, day=21, hour=10), route='M15')
    def thaw_one(self, date_pointer, **kwargs):
        return

#-------------------------------------------------------------------------------------------------------------------------------------
# Barrels in a DataStore are rendered into Deliveries
#-------------------------------------------------------------------------------------------------------------------------------------

# a DataLake instance represents all of the Barrels
class DataStore(GenericStore):

    def __init__(self):
        super().__init__(kind='lake')
        self.Barrels = self.load_barrels()
        # future other metadata -- array of dates and hours covered, total # of records, etc.

    # # TODO COALFACE 1 DataStore.make_barrels
    # # dump each response to data/puddle/YYYY/MM/DD/HH/route_id/barrel_2021-04-03T12:12:12.dat
    # def make_barrels(self, feeds, date_pointer):
    #
    #     # # OLD MAKE_PUDDLES CODE
    #     #
    #     # for route_report in feeds:
    #     #
    #     #     for route_id,route_data in route_report.items():
    #     #
    #     #         route=route_id.split('_')[1]
    #     #         puddle_date_pointer=DatePointer(date_pointer.timestamp,route)
    #     #
    #     #         try:
    #     #
    #     #             # prepare the puddle
    #     #             folder = Puddle(puddle_date_pointer).path
    #     #             filename = 'drop_{}_{}.json'.format(puddle_date_pointer.route, puddle_date_pointer.timestamp).replace(' ', 'T')
    #     #             filepath = folder / PurePath(filename)
    #     #
    #     #             # parse the response
    #     #             route_data = route_data.json()
    #     #
    #     #             # write it
    #     #             with open(filepath, 'wt', encoding="ascii") as f:
    #     #                 json.dump(route_data, f, indent=4)
    #     #
    #     #         except Exception as e: # no vehicle activity?
    #     #             # print (e)
    #     #             # pass
    #     #             import sys
    #     #             sys.exit()
    #     return
    #
    #     # # OLD MAKE_BARREL CODE
    #     #
    #     #     # future this could be abstracted into GenericStore by passing kind in and using generic names?
    #     #     # builds a list of paths to barrels — e.g. hourly route folders of dat files
    #     #     def load_barrels(self):
    #     #         files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['barrel']), recursive=True)
    #     #         dirs = filter(lambda f: os.path.isdir(f), files)
    #     #
    #     #         barrels = []
    #     #         for d in dirs:
    #     #             date_pointer = self.date_pointer_from_a_path(d) #todo will need this as well
    #     #             route = d.split('/')[-1]
    #     #             barrels.append(Barrel(date_pointer, route))
    #     #
    #     #         return barrels
    #     #
    #     #     # TODO TEST
    #     #     # creates a list of all puddles that are not in current hour and can be archived
    #     #     def list_expired_barrels(self):
    #     #         expired_barrels = []
    #     #         for barrel in self.barrels:
    #     #             now_date_pointer = self.timestamp_to_date_pointer(datetime.datetime.now())
    #     #             if barrel.date_pointer != now_date_pointer:
    #     #                 expired_barrels.append(barrel)
    #     #         return expired_barrels
    #     #
    #     #     # TODO TEST
    #     #     # fire the render processes for all expired barrels (not the current hour)
    #     #     def render_barrels(self):
    #     #         barrels_to_render = self.list_expired_barrels()
    #     #         for barrel in barrels_to_render:
    #     #             barrel.render_myself_to_static()
    #     # return



    # builds a list of paths to puddles — e.g. hourly route folders of JSON files
    def load_barrels(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['barrel']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        barrels = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            date_pointer.route = d.split('/')[-1]
            barrels.append(Barrel(date_pointer))
        return barrels

    # creates a list of all barrels that are not in current hour and can be archived
    #bug sort list from oldest to newest, currently seems to work the other way
    def list_expired_barrels(self):
        expired_barrels = []
        for puddle in self.barrels:
            bottom_of_hour = DatePointer(datetime.datetime.now())
            bottom_of_hour.route=puddle.route
            if puddle.date_pointer != bottom_of_hour:
                expired_barrels.append(puddle)
        return expired_barrels


    # load each barrel, extract the pickled buses, bundle them into a new list and then write that to a JSON container
    def render_barrels(self):
        print('firing DataStore.render_barrels')
        barrels_to_archive = self.list_expired_barrels()
        for barrels in barrels_to_archive:
            barrels.render_myself_to_shipment()


        # quickly scan the whole datastore and delete empty folders
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(Path.cwd() / pathmap['store']))):
            if not (files or dirnames):
                os.rmdir(dirpath)

        return

# a Barrel is a folder holding files with multple pickled BusObservation instances parsed from responses for a single route, single hour
class Barrel(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='barrel')
        self.date_pointer = date_pointer
        # fail to create if date_pointer.route doesn't exist
        if date_pointer.route is False:
            raise Exception ('tried to instantiate Barrel because you called __init__ without a value in DatePointer.route')
        self.route = date_pointer.route


    # # TODO COALFACE2 Barrel.render_myself_to_shipment
    # def render_myself_to_shipment(self):
    #
    #     # rough code
    #     pickles_to_render=[x for x in glob("*.dat") if x.is_file()]
    #     pickle_array = []
    #     for picklefile in pickles_to_render:
    #         # print('checking {}'.format(picklefile))
    #         with open(picklefile, 'rb') as pickle_file:
    #             barrel = pickle.load(pickle_file)
    #             for p in barrel:
    #                 pickle_array.append(p)
    #             print ('added {} to route pickle'.format(picklefile))
    #
    #     #iterate over pickle_array and insert into JSON
    #     serial_array=[]
    #     for p in pickle_array:
    #         serial_array.append(p.to_serial())
    #
    #     json_container = dict()
    #     json_container['buses'] = serial_array
    #
    #     outfile = self.archive_path / 'BusObservations-{}-{}-{}-{}-{}.json.tar.gz'.format(self.date_pointer.year,
    #                                                           self.date_pointer.month,
    #                                                           self.date_pointer.day,
    #                                                           self.date_pointer.hour,
    #                                                           self.route
    #                                                           )
    #     with open(outfile, 'w') as f:
    #         json.dump(json_container, f)
    #
    #     # print ('rendered {} pickles from {} files in {} barrel and dumped to static file {}'.format(len(pickle_array), len (picklefile_list), route, static_path+rendered_file))
    #     # print ('made a static JSON file called {} out of {}'.format(outfile,[x for x in pickles_to_render]))
    #     # self.delete_folder() #delete route folder works ok, but # bug delete the empty folders above? e.g. data/lake/puddles/YYYY/MM/DD/HH
    #
    #     return
        
# an Shipment is a rendered Barrel, e.g. a bunch of different BusObservations concatenated together
class Shipment(GenericFolder):

    def __init__(self, date_pointer, kind='shipment'):
        super().__init__(date_pointer, kind='shipment')
        self.date_pointer = date_pointer
        self.route = date_pointer.route
        self.exist, self.filepath = self.check_exist()
        if self.exist == True:
            raise OSError ('there is already a Shipment at {}'.format(self.filepath))

    def check_exist(self):
        filepath = self.path / 'glacier_{}_{}-{}-{}-{}.tar.gz'.format(self.date_pointer.route,
                                                                      self.date_pointer.year,
                                                                      self.date_pointer.month,
                                                                      self.date_pointer.day,
                                                                      self.date_pointer.hour
                                                                      )
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)

    # todo write Shipment.get_one
    # retrieves the appropriate file, optionally uncompresses it?
    # e.g. data = Shipment.ship_one(DatePointer(datetime.datetime(year=2021, month=5, day=21, hour=10), route='M15')
    def ship_one(self, date_pointer, **kwargs):
        return

