from common.Models import *

cwd = Path.cwd()
topdir=cwd / PurePath('reprocessor')
input_path=topdir / 'input'
output_path=topdir / 'output'
store = DataStore(output_path)

store.render_barrels()
