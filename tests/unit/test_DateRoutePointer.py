import datetime, os

from common.Models import DateRoutePointer

def test_get_purepath():
    now = datetime.datetime.now()
    route='M15'
    dp = DateRoutePointer(now,route)
    assert str(dp.purepath) == '{}/{}/{}/{}/{}'.format(now.year, now.month, now.day, now.hour, route)

