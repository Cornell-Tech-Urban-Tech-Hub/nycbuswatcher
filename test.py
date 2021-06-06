import shared.Dumpers as dump
import argparse

lake = dump.DataLake()

print('considering {} puddles to archive'.format( len(lake.puddles) ) )

lake.archive_puddles()


# store = dump.DataStore(runtime_args)

# print('considering {} barrels to render'.format( len(store.barrels) ) )

# store.render_barrels()
