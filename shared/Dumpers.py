import datetime
import sys
import os
import glob
import datetime as dt
import json
import pickle
import geojson
import tarfile
import os.path
from glob import glob


from shared.BusObservation import BusObservation

def checkpath(path):

    # make sure any paths we return exist, or create them
    check = os.path.isdir(path)
    if not check:
        os.makedirs(path)
        print("created folder : ", path)
    else:
        pass
    return

def get_dumppath(path_prefix,route):

    now = dt.datetime.now()
    dumppath = ('').join([path_prefix,
                           str(now.year),
                           '/',
                           str(now.month),
                           '/',
                           str(now.day),
                           '/',
                           str(now.hour),
                            '/',
                            route,
                            '/'
    ])

    checkpath(dumppath)

    return dumppath

def to_barrel(feeds, timestamp):

    # dump each pickle to data/barrel/route_id/barrel_2021-04-03T12:12:12.dat
    for route_report in feeds:
        for route_id,route_data in route_report.items():
            route=route_id.split('_')[1]
            pickles=[]
            try:
                route_data = route_data.json()

                path_prefix = ('data/barrel/')
                barrel_folder = get_dumppath(path_prefix, route)

                barrelfile = barrel_folder + \
                             'barrel_{}_{}.dat'.format(route,timestamp)

                for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                    bus = BusObservation(route, monitored_vehicle_journey)
                    pickles.append(bus)
                with open(barrelfile, "wb") as f:
                    pickle.dump(pickles, f)
            except KeyError: # no VehicleActivity?
                # print ('i didnt dump barrel file for {}'.format(route_id.split('_')[1]))
                pass

    return

def to_files(feeds, timestamp):
    for route_report in feeds:
        # dump each route's response as a raw JSON file
        for route_id,route_data in route_report.items():
            route=route_id.split('_')[1]
            try:
                route_data = route_data.json()

                path_prefix = ('data/response/')
                response_folder = get_dumppath(path_prefix, route)
                responsefile = response_folder + \
                               'reponse_{}_{}.json'.format(route,timestamp)

                with open(responsefile, 'wt', encoding="ascii") as f:
                    json.dump(route_data, f)
            except Exception as e: # no vehicle activity?
                print (e)
                pass
    return

def render_barrel():

    rootdir='data/barrel/'

    hour_folder_pathlist = []

    depth = 3
    for root,dirs,files in os.walk(rootdir):
        if root[len(rootdir):].count(os.sep) < depth:
            for d in dirs:
                hour_folder_pathlist.append(os.path.join(root,d))

    # remove the first 3 which are
    # 'data/barrel/YYYY/'
    # 'data/barrel/YYYY/M/D'
    # 'data/barrel/YYYY/M/D/HH'
    hour_folder_pathlist = hour_folder_pathlist[3:]

    # setup the exclude path
    now = datetime.datetime.now()
    excluded_path = ('{}{}/{}/{}/{}').format(rootdir, str(now.year), str(now.month), str(now.day), str(now.hour))

    # verify that we ARE NOT rendering the folder for the current hour (maybe run this every 30 or 45 minutes then to ensure an offset?)
    # makesure that excludepath is not IN hour_folder_pathlist and remove it
    try:
        hour_folder_pathlist.remove(excluded_path)
    except:
        print ('error: cannot render current hour folder')
        sys.exit()
        pass

    #bug debugged to here-------------------------------------------------------------

    # make sure we ARE rendering any folders that did not get rendered earlier
    for hour_folder in hour_folder_pathlist:

        route_folder_list = [(x.name,x.path) for x in os.scandir(hour_folder) if x.is_dir()]
        for route, route_folder in route_folder_list:

            # load all the pickle files into a single array
            picklefile_list = glob("{}*.dat".format(route_folder+'/'))

            pickle_array = []
            for picklefile in picklefile_list:
                print('checking {}'.format(picklefile))
                with open(picklefile, 'rb') as pickle_file:
                    barrel = pickle.load(pickle_file)
                    for p in barrel:
                        pickle_array.append(p)
                    print ('added {} to route pickle'.format(picklefile))

            #todo 1 make a template for valid JSON
            json_container = dict()

            #todo 2 iterate over pickle_array and insert into JSON
            json_array=[]
            for p in pickle_array:
                json_array.append(p.to_dict())

            json_container['buses'] = json_array

            path_prefix='static/'
            static_path=route_folder.replace('data/barrel','static')
            checkpath(static_path)

            rendered_file='rendered_barrel_{}.json'.format(route) #todo add date to filename?
            with open(static_path+'/'+rendered_file, 'w') as f:
                json.dump(json_array, f)
            print ('rendered {} pickles from {} files in this barrel and dumped to static file {}'.format(len(pickle_array), len (picklefile_list), static_path+rendered_file))

        #remove everything in the response directory (but what if something is writing to it during the run?)
        #     for file in yesterday_gz_files:
        #         try:
        #             os.remove(file)
        #         except:
        #             pass
    return

'''
def render_responses():  #todo


    #todo use the path crawling algoritm like above

    # bundle up everything in ./data/responses/*.json into a tarball into ./data/archive
    # https://programmersought.com/article/77402568604/

    tarballpath = './data/archive'
    raw_response_path = ''.join(['data/',get_dumppaths()['responsepath'],'**/*.json'])

    files_to_archive = glob.glob(raw_response_path, recursive=True)

    outfile=
    with tarfile.open(outfile, "w:gz") as tar:
        for file in yesterday_gz_files:
            tar.add(file)

    #     print ('made a tarball of {} files from {} into {}'.format(len(yesterday_gz_files), yesterday, outfile))

    #     #remove everything in the response directory (but what if something is writing to it during the run?)
    #
    #     for file in yesterday_gz_files:
    #         try:
    #             os.remove(file)
    #         except:
    #             pass
    return
'''

def to_lastknownpositions(feeds): #future please refactor me
    f_list=[]
    for route_bundle in feeds:
        for route_id,route_report in route_bundle.items():
            try:
                route_report = route_report.json() #bug sometimes get a Json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
                for b in route_report['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                    p = geojson.Point((b['MonitoredVehicleJourney']['VehicleLocation']['Longitude'],
                                       b['MonitoredVehicleJourney']['VehicleLocation']['Latitude']))

                    try:
                        occupancy={'occupancy':b['MonitoredVehicleJourney']['Occupancy']}
                    except KeyError:
                        occupancy = {'occupancy': 'empty'}

                    try:
                        passenger_count={'passenger_count': int(b['MonitoredVehicleJourney']['MonitoredCall']['Extensions'][
                            'Capacities']['EstimatedPassengerCount'])}
                    except KeyError:
                        passenger_count = {'passenger_count': 0}

                    try:
                        lineref={'lineref':b['MonitoredVehicleJourney']['LineRef']}
                    except KeyError:
                        lineref = {'lineref': 'n/a'}

                    try:
                        vehicleref={'vehicleref':b['MonitoredVehicleJourney']['VehicleRef']}
                    except KeyError:
                        vehicleref = {'vehicleref': 'n/a'}

                    try:
                        trip_id={'trip_id':b['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']}
                    except:
                        trip_id = {'trip_id': 'n/a'}

                    try:
                        next_stop_id={'next_stop_id':b['MonitoredVehicleJourney']['MonitoredCall']['StopPointRef']}
                    except:
                        next_stop_id = {'next_stop_id': 'n/a'}

                    try:
                        next_stop_eta={'next_stop_eta':b['MonitoredVehicleJourney']['MonitoredCall']['ExpectedArrivalTime']}
                    except:
                        next_stop_eta = {'next_stop_eta': 'n/a'}

                    try:
                        next_stop_d_along_route={'next_stop_d_along_route':b['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Distances'][
                        'CallDistanceAlongRoute']}
                    except:
                        next_stop_d_along_route = {'next_stop_d_along_route': 'n/a'}

                    try:
                        next_stop_d={'next_stop_d':b['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Distances']['DistanceFromCall']}
                    except:
                        next_stop_d = {'next_stop_d': 'n/a'}


                    f = geojson.Feature(geometry=p, properties={**occupancy,**passenger_count,**lineref,**trip_id,**vehicleref,**next_stop_id,**next_stop_eta,**next_stop_d_along_route,**next_stop_d})

                    f_list.append(f)
            except KeyError: # no VehicleActivity?
                pass
    fc = geojson.feature.FeatureCollection(f_list)

    with open('./static/lastknownpositions.geojson', 'w') as outfile:
        geojson.dump(fc, outfile)

    return



