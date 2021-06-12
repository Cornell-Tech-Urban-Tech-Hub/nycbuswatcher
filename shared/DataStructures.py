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


class GenericStore():

    def __init__(self, kind=None):
        super().__init__()
        self.path = self.get_path(pathmap[kind])
        self.checkpath(self.path)

    def get_path(self, prefix):
        folderpath = Path.cwd() / prefix
        self.checkpath(folderpath)
        return folderpath

    def checkpath(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return

    def date_pointer_from_a_path(self, apath):
        parts = apath.split('/')
        return DatePointer(datetime.datetime(year=int(parts[-5]),
                                    month=int(parts[-4]),
                                    day=int(parts[-3]),
                                    hour=int(parts[-2])
                                    ))



class GenericFolder():

    def __init__(self, date_pointer, kind=None):
        super().__init__()
        self.path = self.get_path(pathmap[kind], date_pointer)
        self.checkpath(self.path)

    def get_path(self, prefix, date_pointer):
        folderpath = Path.cwd() / prefix / date_pointer.purepath
        self.checkpath(folderpath)
        return folderpath

    def checkpath(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        return

    # after https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    def delete_folder(self):
        def rm_tree(pth):
            for child in pth.glob('*'):
                if child.is_file():
                    child.unlink()
                else:
                    rm_tree(child)
            pth.rmdir()
        rm_tree(self.path)
        return


class DataLake(GenericStore):

    def __init__(self):
        super().__init__(kind='lake')
        self.puddles = self.load_puddles()
        # future other metadata -- array of dates and hours covered, total # of records, etc.

    def make_puddles(self, feeds, date_pointer):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route=route_id.split('_')[1]
                puddle_date_pointer=DatePointer(date_pointer.timestamp, route)
                try:
                    folder = Puddle(puddle_date_pointer).path
                    filename = 'drop_{}_{}.json'.format(puddle_date_pointer.route, puddle_date_pointer.timestamp).replace(' ', 'T')
                    filepath = folder / PurePath(filename)
                    route_data = route_data.json()
                    with open(filepath, 'wt', encoding="ascii") as f:
                        json.dump(route_data, f, indent=4)
                except Exception as e: # no vehicle activity?
                    print (e)
                    pass
        return

    def load_puddles(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['puddle']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        puddles = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            date_pointer.route = d.split('/')[-1]
            p = Puddle(date_pointer)
            p.path = p.path / PurePath (date_pointer.route)
            puddles.append(p)
        return puddles

    #TODO COALFACE -- MAKE SURE I WORK WHEN TRIGGERED BY APSCHEDULER
    def list_expired_puddles(self):
        expired_puddles = []
        for puddle in self.puddles:
            # print('is puddle {} expired?'.format(puddle.path))
            bottom_of_hour = DatePointer(datetime.datetime.now())
            bottom_of_hour.route=puddle.route
            # print ('comparing DatePointers: bottom_of_hour {} vs  puddle.date_pointer {}'.format(bottom_of_hour,puddle.date_pointer))
            if str(puddle.date_pointer) != str(bottom_of_hour):
                print('EXPIRED  puddle.date_pointer {}'.format(puddle.date_pointer))
                expired_puddles.append(puddle)
        return expired_puddles

    def freeze_puddles(self):
        # print('firing DataLake.freeze_puddles')
        puddles_to_archive = self.list_expired_puddles()
        if len(puddles_to_archive) == 0:
            print('no expired puddles to freeze')
            return
        for puddle in puddles_to_archive:
            puddle.freeze_myself_to_glacier()
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(Path.cwd() / pathmap['lake']))):
            if not (files or dirnames):
                os.rmdir(dirpath)
        return


class Puddle(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='puddle')
        self.date_pointer = date_pointer
        if date_pointer.route is False:
            raise Exception ('tried to instantiate Puddle because you called __init__ without a value in DatePointer.route')
        self.route = date_pointer.route


    def freeze_myself_to_glacier(self):
        drops_to_freeze=[x for x in self.path.glob('*.json') if x.is_file()]
        try:
            outfile = Glacier(self.date_pointer)
        except OSError:
            # future write handler for partially rendered puddles(maybe a different filename, or move it to a lost+found?)
            return
        with tarfile.open(outfile.filepath, "w:gz") as tar:
                for drop in drops_to_freeze:
                    tar.add(drop, arcname=drop.name.replace(':','-')) # tar doesnt like colons
        print ('froze {} drops to Glacier at {}'.format(len(drops_to_freeze), outfile.path))
        self.delete_folder()
        return


class Glacier(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='glacier')
        self.date_pointer = date_pointer
        self.route = date_pointer.route
        self.exist, self.filepath = self.check_exist()
        if self.exist == True:
            raise OSError ('there is already an archive at {}'.format(self.filepath))

    def check_exist(self):
        filepath = self.path / 'glacier_{}_{}-{}-{}-{}.tar.gz'.\
            format(self.date_pointer.route,self.date_pointer.year,
                   self.date_pointer.month,self.date_pointer.day,self.date_pointer.hour)
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)

    # def thaw_one(self, date_pointer, **kwargs):
    #     # retrieves the appropriate file, optionally uncompresses it?
    #     # e.g. data = Glacier.thaw_one(DatePointer(datetime.datetime(year=2021, month=5, day=21, hour=10), route='M15')
    #     return


class DataStore(GenericStore):

    def __init__(self):
        super().__init__(kind='store')
        self.barrels = self.load_barrels()
        self.shipments = self.load_shipments()
        # future other metadata -- array of dates and hours covered, total # of records, etc.

    def make_barrels(self, feeds, date_pointer):
        for route_report in feeds:
            for route_id,route_data in route_report.items():
                route = route_id.split('_')[1]
                barrel_date_pointer=DatePointer(date_pointer.timestamp, route)
                pickles = []
                try:
                    folder = Barrel(barrel_date_pointer).path
                    filename = 'pickle_{}_{}.dat'.format(barrel_date_pointer.route, barrel_date_pointer.timestamp).replace(' ', 'T')
                    filepath = folder / PurePath(filename)
                    route_data = route_data.json()
                    for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                        bus = BusObservation(route, monitored_vehicle_journey)
                        pickles.append(bus)
                    with open(filepath, "wb") as f:
                        pickle.dump(pickles, f)
                except KeyError:
                    # future find a way to filter these out to reduce overhead
                    # this is almost always the result of a route that doesn't exist, so why is it in the OBA response?
                    pass
                except json.JSONDecodeError:
                    # this is probably no response?
                    print ('JSONDecodeError: No/Bad API response? - route {}'.format(route))
                    pass
        return

    def load_barrels(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['barrel']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        barrels = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            date_pointer.route = d.split('/')[-1]
            b = Barrel(date_pointer)
            b.path = b.path / PurePath (date_pointer.route)
            barrels.append(b)
        return barrels

    def load_shipments(self):
        files = glob('{}/*/*/*/*/*'.format(Path.cwd() / pathmap['shipment']), recursive=True)
        dirs = filter(lambda f: os.path.isdir(f), files)
        shipments = []
        for d in dirs:
            date_pointer = self.date_pointer_from_a_path(d)
            date_pointer.route = d.split('/')[-1]
            s = Shipment(date_pointer)
            s.path = s.path / PurePath (date_pointer.route) #if fails, use s.filepath instead?
            shipments.append(s)
        return shipments

    def list_expired_barrels(self):
        expired_barrels = []
        self.barrels = self.load_barrels() #reload here in case this was executed from apscheduler
        for puddle in self.barrels:
            bottom_of_hour = DatePointer(datetime.datetime.now())
            bottom_of_hour.route=puddle.route
            if str(puddle.date_pointer) != str(bottom_of_hour): #bug this doesn't exclude anything from current hour
                expired_barrels.append(puddle)
        return expired_barrels  # future sort list from oldest to newest

    def render_barrels(self):
        # print('firing DataStore.render_barrels')
        barrels_to_archive = self.list_expired_barrels()
        if len(barrels_to_archive) == 0:
            print('no expired barrels to render')
            return
        for barrel in barrels_to_archive:
            barrel.render_myself_to_shipment()
        # https://gist.github.com/roddds/aff960f47d4d1dffba2235cc34cb45fb
        for dirpath, dirnames, files in os.walk( (str(Path.cwd() / pathmap['store']))):
            if not (files or dirnames):
                os.rmdir(dirpath)
        return

    # def list_routes(self,date_pointer):
    #     folder = self.path / date_pointer.purepath
    #     subfolders = [p for p in folder.iterdir() if p.is_dir()]
    #     # print (subfolders)
    #     return subfolders
    #

    def list_routes(self, date_pointer_query):
        routes = []
        self.shipments = self.load_shipments() #reload here Just In Case
        dp1=date_pointer_query
        for shipment in self.shipments:
            dp2=shipment.date_pointer
            print ('comparing {} and {}'.format(dp1,dp2))
            if dp1.year == dp2.year:
                if dp1.month == dp2.month:
                    if dp1.day == dp2.day:
                        if dp1.hour == dp2.hour:
                            routes.append(shipment.route)
        return routes



class Barrel(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='barrel')
        self.date_pointer = date_pointer
        if date_pointer.route is False:
            raise Exception ('tried to instantiate Barrel because you called __init__ without a value in DatePointer.route')
        self.route = date_pointer.route

    def render_myself_to_shipment(self):
        pickles_to_render=[x for x in self.path.glob('*.dat') if x.is_file()]
        # print('rendering {} picklefiles to Shipment for Barrel {}'.format(len(pickles_to_render),self.path))
        try:
            outfile = Shipment(self.date_pointer)
        except OSError:
            # future write handler for partially rendered puddles(maybe a different filename, or move it to a lost+found?)
            return
        pickle_array = []
        for picklefile in pickles_to_render:
            with open(picklefile, 'rb') as pickle_file:
                barrel = pickle.load(pickle_file)
                for p in barrel:
                    pickle_array.append(p)
                # print ('added {} to route pickle'.format(picklefile))
        serial_array=[]
        for p in pickle_array:
            serial_array.append(p.to_serial())
        json_container = dict()
        json_container['buses'] = serial_array
        with open(outfile.filepath, 'w') as f:
            json.dump(json_container, f, indent=4)
        print ('wrote {} pickles to Shipment at {}'.format(len(pickle_array), outfile.filepath))
        self.delete_folder()
        return


class Shipment(GenericFolder):

    def __init__(self, date_pointer):
        super().__init__(date_pointer, kind='shipment')
        self.date_pointer = date_pointer
        self.route = date_pointer.route
        self.exist, self.filepath = self.check_exist()
        if self.exist == True:
            raise OSError ('there is already a Shipment at {}'.format(self.filepath))

    def check_exist(self):
        filepath = self.path / 'shipment_{}_{}-{}-{}-{}.json'.\
            format(self.date_pointer.route,self.date_pointer.year,
                   self.date_pointer.month,self.date_pointer.day,self.date_pointer.hour)
        if filepath.is_file() is True:
            return (True, filepath)
        elif filepath.is_file() is False:
            return (False, filepath)

    # def ship_one(self, date_pointer, **kwargs):
    #     # retrieves the appropriate file, optionally uncompresses it?
    #     # e.g. data = Shipment.ship_one(DatePointer(datetime.datetime(year=2021, month=5, day=21, hour=10), route='M15')
    #     return

