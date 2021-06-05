import shared.Dumpers as dump
import argparse

parser = argparse.ArgumentParser(description='NYCbuswatcher DateLake test')
parser.add_argument('--dry-run', action="store_true", dest="dry-run", help="Force dry run (dont write or delete anything) ")
runtime_args = parser.parse_args()

lake = dump.DataLake(runtime_args)
print('considering {} puddles to archive'.format( len(lake.puddles) ) )
lake.archive_puddles()

# todo uncomment me
# store = dump.DataStore(runtime_args)
# print('considering {} barrels to render'.format( len(store.barrels) ) )
# store.render_barrels()
