import shared.Models as data

lake = data.DataLake()
lake.freeze_puddles()

store = data.DataStore()

store.render_barrels()
store.dump_dashboard()