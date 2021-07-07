from common.Models import *

cwd = Path.cwd()

lake = DataLake(cwd)
lake.freeze_puddles()

store = DataStore(cwd)
store.render_barrels()
store.dump_dashboard()