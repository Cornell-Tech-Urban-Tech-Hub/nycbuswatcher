import datetime, os

from shared.Models import DatePointer

def test_get_purepath():
    now = datetime.datetime.now()
    dp = DatePointer(now)
    assert str(dp.purepath) == '{}/{}/{}/{}'.format(now.year, now.month, now.day, now.hour)
