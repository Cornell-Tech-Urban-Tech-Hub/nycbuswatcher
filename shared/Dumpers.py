import requests
import os
import glob
import datetime as dt
import json
import gzip
import pickle
import geojson
import tarfile
import os.path

from shared.BusObservation import BusObservation

def dir_check(checkpath):
    check = os.path.isdir(checkpath)
    if not check:
        os.makedirs(checkpath)
        print("created folder : ", checkpath)
    else:
        pass
    return

def get_dumppaths(rt):

    barrelpath='data/barrel/{}'.format(rt)
    responsepath='data/responses/{}'.format(rt)
    archivepath='data/archive/{}'.format(rt)

    now = dt.datetime.now()
    staticpath = ('').join(['static/',
                           str(now.year),
                           '/',
                           str(now.month),
                           '/',
                           str(now.day)])


    dumppaths = {
        'barrelpath': barrelpath,
        'responsepath': responsepath,
        'archivepath': archivepath,
        'staticpath' : staticpath
    }

    # make sure any paths we return exist, or create them
    for pathitem, checkpath in dumppaths.items():
        dir_check(checkpath)

    return dumppaths

def to_barrel(feeds, timestamp):

    # dump each pickle to data/barrel/route_id/barrel_2021-04-03T12:12:12.dat
    for route_report in feeds:
        for route_id,route_data in route_report.items():
            pickles=[]
            try:
                route_data = route_data.json()
                barrel='{}/barrel_{}_{}.dat'.format(get_dumppaths(route_id.split('_')[1])['barrelpath'],
                                                    route_id.split('_')[1],
                                                    timestamp)
                for monitored_vehicle_journey in route_data['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'][0]['VehicleActivity']:
                    bus = BusObservation(route_id, monitored_vehicle_journey)
                    pickles.append(bus)
                with open(barrel, "wb") as f:
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
                outfile='{}/response_{}_{}.json'.format(get_dumppaths(route_id.split('_')[1])['responsepath'],
                                                        route_id.split('_')[1],
                                                        timestamp
                                                        )

                with open(outfile, 'wt', encoding="ascii") as f:
                    json.dump(route_data, f)
            except Exception as e: # no vehicle activity?
                print (e)
                pass
    return

# def render_barrel(): #todo
#
#     json_template = {'buses': None}
#
#     picklefile_list = glob.glob("{}*.dat".format(get_dumppaths(route_id)['barrelpath']))
#     pickle_array = []
#     for picklefile in picklefile_list:
#         pickle_array.append(pickle.load(picklefile))
#
#     # todo dump the pickle_array to a static json file in the fastapi's static folder
#
#     renderfile_path='' # year/month/day/hour
#     renderfile_name='' # year_month_day_hour_route
#
#     print('fetched {} pickles from {} files in the barrel and dumped to static file {}'.format(len(pickle_array), len (picklefile_list), renderfile_path+renderfile_name)
#
#     return

# def tarball_responses():  #todo
#     # bundle up ./data/responses/*json into a tarball into ./data/archive
#     # https://programmersought.com/article/77402568604/
#
#     tarballpath = './data/archive'
#     raw_response_path = ('').join(['data/', )
#
#     #     print ('made a tarball of {} files from {} into {}'.format(len(yesterday_gz_files), yesterday, outfile))
#     #
#     #     with tarfile.open(outfile, "w:gz") as tar:
#     #         for file in yesterday_gz_files:
#     #             tar.add(file)
#     #
#     #     #remove all files rotated
#     #
#     #     for file in yesterday_gz_files:
#     #         try:
#     #             os.remove(file)
#     #         except:
#     #             pass
#     return

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



