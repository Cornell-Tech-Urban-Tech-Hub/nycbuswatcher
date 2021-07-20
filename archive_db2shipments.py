# archive_db2shipments.py
# july 2021
# dumps shipment files from a collection of v1 nycbuswatcher database tables of identical format to ./tmp

import datetime as dt
import argparse
import itertools
from collections import defaultdict
from sqlalchemy import (Column, Date, Integer, MetaData, Table, String, create_engine, select, insert)
from common.Models import *

######################################################

# define datespace
date1 = '2020-10-16'
date2 = '2021-05-31'

# define tablespace
tablespace=['buses_reprocessed_2020_10',
			'buses_reprocessed_2020_11',
			'buses_reprocessed_2020_12_a',
			'buses_reprocessed_2020_12_b',
			'buses_reprocessed_2021_01_a',
			'buses_reprocessed_2021_01_b',
			'buses_reprocessed_2021_02_a',
			'buses_reprocessed_2021_02_b',
			'buses_reprocessed_2021_03_a',
			'buses_reprocessed_2021_03_b',
			'buses_reprocessed_2021_04_a',
			'buses_reprocessed_2021_04_b',
			'buses_reprocessed_dumped_2021_05',
			'buses_reprocessed_dumped_2021_06'
			]

# define template

def make_monitored_vehicle_journey(bus,data):
	return \
		{
			"MonitoredVehicleJourney": {
				"LineRef": f'{bus.route_long}',
				"DirectionRef": f'{bus.direction}',
				"FramedVehicleJourneyRef": {
					"DataFrameRef": f'{bus.service_date}',
					"DatedVehicleJourneyRef": f'{bus.trip_id}'
				},
				"JourneyPatternRef": f'{bus.gtfs_shape_id}',
				"PublishedLineName": f'{bus.route_short}',
				"OperatorRef": f'{bus.agency}',
				"OriginRef": f'{bus.origin_id}',
				"DestinationName": f'{bus.destination_name}',
				"OriginAimedDepartureTime": "2021-07-13T17:57:00.000-04:00",
				"SituationRef": [], #bug how to parse -- 'alert': ['SituationRef', 'SituationSimpleRef']
				"VehicleLocation": {
					"Longitude": f'{bus.lon}',
					"Latitude": f'{bus.lat}'
				},
				"Bearing": f'{bus.bearing}',
				"ProgressRate": f'{bus.progress_rate}',
				"ProgressStatus": f'{bus.progress_status}',
				"BlockRef": f'{bus.gtfs_block_id}',
				"VehicleRef": f'{bus.vehicle_id}',
				"MonitoredCall": {
					"Extensions": {
						"Capacities": {
							"EstimatedPassengerCount": f'{bus.passenger_count}',
							"DistanceFromCall": f'{bus.next_stop_d}',
							"CallDistanceAlongRoute": f'{bus.next_stop_d_along_route}'
						}
					}
				}
			},
			"RecordedAtTime": f'{bus.timestamp.isoformat()}' #bug compare with standard shipment
		}


######################################################

def get_datelist(date1,date2):
	datelist = []
	start = dt.datetime.strptime(date1, '%Y-%m-%d')
	end = dt.datetime.strptime(date2, '%Y-%m-%d')
	step = dt.timedelta(days=1)
	while start <= end:
		for hr in range(24):
			datelist.append(
				DatePointer(
					start+dt.timedelta(hours=hr)
				)
			)
		start += step
	return datelist

def get_hour_of_data(table, datehour):

	with engine.connect() as conn:
		hour_of_data = []
		date_str=datehour.timestamp.strftime('%Y-%m-%d')
		# todo might be more consistent to have both conditions query against timestamp
		query = f"""SELECT * FROM {table} WHERE service_date='{date_str}' AND HOUR(timestamp) = {datehour.hour}"""
		print(f'querying: table {table} \t\t date {date_str} \t hour {datehour.hour} \t completed at {dt.datetime.now()}')
		results = conn.execute(query)
		return [x for x in results]


def dump_hour(store, hour_of_data):

		# iterate over the groups
		for route_long, route_group in itertools.groupby(hour_of_data, lambda x: x.route_long):

			data = defaultdict()
			data['VehicleActivity'] = []

			for bus in route_group:
				# process the route_group into a feed and feed it to store.make_barrels


				data['VehicleActivity'].append(make_monitored_vehicle_journey(bus,data))

				fake_response =  {"Siri" :{
					"ServiceDelivery" : {
						"VehicleMonitoringDelivery" : []
					}
				}
			}


				fake_response['Siri']['ServiceDelivery']['VehicleMonitoringDelivery'].append(data)

				feeds = [{ route_long: fake_response} ]
				store.make_barrels(feeds, datehour)

		store.render_barrels()
		return


if __name__=="__main__":

	# parse arguments
	parser = argparse.ArgumentParser(description='NYCbuswatcher shipment dumper, dumps fromv1 database table to shipment json files')
	parser.add_argument('-l', '--localhost', action='store_true', help="Run on localhost (otherwise uses docker container hostname 'db')")
	args = parser.parse_args()

	# setup the db connection
	if args.localhost == True:
		engine = create_engine('mysql://nycbuswatcher:bustime@localhost:3306/buses')
	else:
		engine = create_engine('mysql://nycbuswatcher:bustime@db:3306/buses')

	#create datelist as list of DatePointer objects by hour


	# todo method 3 create a separate datastore for each table
	# todo figure out how to combine -- then identify, load, and combine any that overlap


	for table in tablespace:

		# create a separate datastore for each table
		store = DataStore(Path.cwd() / 'tmp' / f"""{table}""")

		#get each hour from the table and render it
		for datehour in get_datelist(date1,date2):
			hour_of_data = 	get_hour_of_data(table, datehour)
			dump_hour(store, hour_of_data)

