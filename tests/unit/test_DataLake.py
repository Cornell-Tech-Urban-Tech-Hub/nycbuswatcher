import datetime
from pathlib import Path, PurePath
import pytest
import glob, os, shutil
import json
from common.Models import DatePointer, DataLake

# a DataLake with just one grab in it
@pytest.fixture
def lake():
    lake = DataLake()
    return lake

def test_init_lake():
    print (type(lake))
    assert lake.uid is not None


@pytest.fixture()
def single_current_feed():
    from common.Grabber import async_grab_and_store
    return async_grab_and_store(True) #todo need to refactor async grab and store to not store?
    #todo because async_grab_and_store will create a DataLake and DataStore, could we trick it into using the tmpdir?


# todo test that runs off a fixture containing 2 whole days' worth of data


# bug this doesn't work but not even sure what we are trying to test
# # using tmp_path, which creates a pathlib.Path object (need to make the cwd for my tests)
# def test_make_puddles(tmp_path):
#
#     # 1 copy the sample data '../fixtures/data-2021-06-23T00:04:03/lake/**/*.json' into tmp_path
#     source = Path('../fixtures/data-2021-06-23T00:04:03/lake')
#     destination = tmp_path / PurePath('data/lake')
#     shutil.copytree(source, destination)
#
#     # 2 load the drops into array
#     search_pattern = str(destination)+'/**/*.json'
#     feed_filelist = [f for f in glob.glob(search_pattern, recursive = True)]
#     for file in feed_filelist:
#         feeds = []
#         with open(file, 'r') as f:
#             feeds.append({'M15':json.load(f)})
#
#
#         # # 3 process test data and assert
#         lake = DataLake()
#         lake.path=destination
#
#         lake.make_puddles(feeds,
#                           DatePointer(
#                               datetime.datetime(
#                                   year=2021,
#                                   month=6,
#                                   day=24,
#                                   hour=12)
#                           )
#                           )
#
#     assert len(lake.scan_puddles()) == 241
#     assert True
