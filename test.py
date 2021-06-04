import shared.Dumpers as dump
import argparse

#todo add a dry-run switch that doesn't render anything

parser = argparse.ArgumentParser(description='NYCbuswatcher DateLake test')
parser.add_argument('--dry-run', action="store_true", dest="dry-run", help="Force dry run (dont write or delete anything) ")
runtime_args = parser.parse_args()

lake = dump.DataLake(runtime_args)
print ('rendering {} puddles'.format( len(lake.puddles) ) )

lake.render_puddles()


# store = dump.DataStore()
# print (store.barrels)

