from sqlalchemy import create_engine

from common.Database_db2shipments import *
# load the DataStore
from common.Models import *
store = DataStore(Path.cwd())

# tablenames=['buses_reprocessed_2020_10',
# 			'buses_reprocessed_2020_11',
# 			'buses_reprocessed_2020_12_a',
# 			'buses_reprocessed_2020_12_b',
# 			'buses_reprocessed_2021_01_a',
# 			'buses_reprocessed_2021_01_b',
# 			'buses_reprocessed_2021_02_a',
# 			'buses_reprocessed_2021_02_b',
# 			'buses_reprocessed_2021_03_a',
# 			'buses_reprocessed_2021_03_b',
# 			'buses_reprocessed_2021_04_a',
# 			'buses_reprocessed_2021_04_b',
# 			'buses_dumped_2021_05',
# 			'buses_dumped_2021_06'
# 			]

# for testing
tablenames=['buses_reprocessed_2020_10',
			'buses_reprocessed_2020_11',
			'buses_reprocessed_2020_12_a'
			]


# loop over the tables
for tablename in tablenames:
	# connect to the database
	db_url=get_db_url()
	db_connect = create_engine(db_url)
	print('Connected to {}'.format(db_url))


	# 1 get list of dates and n_buses in the database

	sql_statement="SELECT service_date, count(*) FROM {} GROUP BY service_date;".format(tablename)
	query = conn.execute(sql_statement)
	results = [
		dict(
			zip(
				tuple(
					query.keys()
				), i
			)
		)
		for i in query.cursor
	]

	# 2
	#
	for row in results:
		print(tablename,row)
