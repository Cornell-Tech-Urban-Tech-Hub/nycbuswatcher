from sqlalchemy import create_engine

from common.Database_db2shipments import *
# load the DataStore
from common.Models import *
store = DataStore(Path.cwd())


'''
general approach

1. loop over all the tables and create a new lookup table with the following columns

tablename, record_id, timestamp

2. then iterate over dates from 10-15-2020 to 04-30-2021 through that, pulling records that match from the right tables, concatente them into a shipment file and dump oit 

'''











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


make_single_table_sql="""
# use a union query https://support.microsoft.com/en-us/office/use-a-union-query-to-combine-multiple-queries-into-a-single-result-1f772ec0-cc73-474d-ab10-ad0a75541c6e

CREATE_TABLE buses_reprocessed_2020_10_thru_2021_04

AS

SELECT *
FROM buses_reprocessed_2020_10

UNION

SELECT *
FROM buses_reprocessed_2020_11

UNION

SELECT *
FROM buses_reprocessed_2020_12_a

UNION

SELECT *
FROM buses_reprocessed_2020_12_b

UNION

SELECT *
FROM buses_reprocessed_2021_01_a

UNION

SELECT *
FROM buses_reprocessed_2021_01_b

UNION

SELECT *
FROM buses_reprocessed_2021_02_a

UNION

SELECT *
FROM buses_reprocessed_2021_02_b

UNION

SELECT *
FROM buses_reprocessed_2021_03_a

UNION

SELECT *
FROM buses_reprocessed_2021_03_b

UNION

SELECT *
FROM buses_reprocessed_2021_04_a

UNION

SELECT *
FROM buses_reprocessed_2021_04_b

;"""






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
