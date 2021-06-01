import requests
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

def get_datehourpath(path_prefix):

    now = dt.datetime.now()
    datehourpath = ('').join([
                           str(now.year),
                           '/',
                           str(now.month),
                           '/',
                           str(now.day),
                           '/',
                           str(now.hour),
                            '/'
    ])

    # make sure any paths we return exist, or create them
    check = os.path.isdir(path_prefix + datehourpath)
    if not check:
        os.makedirs(path_prefix + datehourpath)
        print("created folder : ", path_prefix + datehourpath)
    else:
        pass

    return path_prefix + datehourpath

def to_barrel(feeds, timestamp):

    # dump each pickle to data/barrel/route_id/barrel_2021-04-03T12:12:12.dat
    for route_report in feeds:
        for route_id,route_data in route_report.items():
            pickles=[]
            try:
                route_data = route_data.json()

                path_prefix = ('data/barrel/' +
                               route_id.split('_')[1] +
                               '/')
                barrel_folder = get_datehourpath(path_prefix)
                barrelfile = barrel_folder + 'barrel_{}_{}.dat'.format(route_id.split('_')[1],timestamp)

                for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                    bus = BusObservation(route_id, monitored_vehicle_journey)
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
            try:
                route_data = route_data.json()

                path_prefix = ('data/response/' +
                               route_id.split('_')[1] +
                               '/')
                response_folder = get_datehourpath(path_prefix)
                responsefile = response_folder + 'reponse_{}_{}.json'.format(route_id.split('_')[1],timestamp)

                with open(responsefile, 'wt', encoding="ascii") as f:
                    json.dump(route_data, f)
            except Exception as e: # no vehicle activity?
                print (e)
                pass
    return

'''
def render_barrel():

    route_folders = glob("{}*".format(get_dumppaths()['barrelpath']))

    for folderpath in route_folders:
        picklefile_list = glob("{}/*.dat".format(folderpath))

        pickle_array = []
        for picklefile in picklefile_list:
            with open(picklefile, 'rb') as pickle_file:
                pickle_array.append(pickle.load(pickle_file))

        #todo ------STOPPED DEBUGGING HERE SUNDAY NIGHT-------------------------------------------------

        # todo pickle the pickle_array in get_dumppaths()['barrelpath'] with a route and date path

        renderfile_template = {'buses': None}


        #
        #     renderfile_path='' # year/month/day/hour
        #     renderfile_name='' # year_month_day_hour_route
        #
        #     print('fetched {} pickles from {} files in the barrel and dumped to static file {}'.format(len(pickle_array), len (picklefile_list), renderfile_path+renderfile_name)
        #
    return

def render_responses():  #todo

    # bundle up everything in ./data/responses/*.json into a tarball into ./data/archive
    # https://programmersought.com/article/77402568604/

    tarballpath = './data/archive'
    raw_response_path = ''.join(['data/',get_dumppaths()['responsepath'],'**/*.json'])

    files_to_archive = glob.glob(raw_response_path, recursive=True)

    outfile=
    with tarfile.open(outfile, "w:gz") as tar:
        for file in yesterday_gz_files:
            tar.add(file)

    # walk and glob

    #     print ('made a tarball of {} files from {} into {}'.format(len(yesterday_gz_files), yesterday, outfile))
    #
    #     with tarfile.open(outfile, "w:gz") as tar:
    #         for file in yesterday_gz_files:
    #             tar.add(file)
    #
    #     #remove all files rotated
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



