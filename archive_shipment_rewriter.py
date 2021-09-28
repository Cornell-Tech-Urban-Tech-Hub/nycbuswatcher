from common.Models import *

store = DataStore(Path.cwd())


for shipment in store.scan_shipments():
    if shipment.month in [10,11,12,1,2,3,4]:

        # make a backup of the file, overwriting backup
        shipment.backup_file()

        # read the shipment into memory
        with open(shipment.filepath, "r+") as f:
            shipment_data = json.load(f)

            # iterate over all records in buses and cast strings to floats
            for bus in shipment_data['buses']:
                bus['lon'] = float(bus['lon'])
                bus['lat'] = float(bus['lat'])

            # overwrite the old data
            f.seek(0)
            f.write(json.dumps(shipment_data, indent=4))
            print(f'wrote new shipment to {shipment.filepath}')
            f.truncate()

# # todo write a restore backup switch?
# # copies the backup back to the main file
#
# for shipment in store.scan_shipments():
#     if shipment.month in [10,11,12,1,2,3,4,5,6]:
#
#         # make a backup of the file, overwriting backup
#         shipment.backup_file()
