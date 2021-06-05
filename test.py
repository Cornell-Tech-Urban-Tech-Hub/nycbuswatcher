import shared.Dumpers as dump
import argparse

parser = argparse.ArgumentParser(description='NYCbuswatcher DateLake test')
parser.add_argument('--dry-run', action="store_true", dest="dry-run", help="Force dry run (dont write or delete anything) ")
runtime_args = parser.parse_args()

lake = dump.DataLake(runtime_args)
print('considering {} puddles to render'.format( len(lake.puddles) ) )

#TODO TEST
lake.archive_puddles()


# store = dump.DataStore(runtime_args)
# print('rendering {} barrels'.format( len(store.barrels) ) )
# # print([str(b.folder.path) for b in store.barrels])