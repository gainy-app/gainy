import os, sys
import datetime
import dateutil

task_dir = os.path.abspath(os.path.join(__file__, '../../../'))
sys.path.append(task_dir)

from actions import UpdatePurchases


def test_get_duration():
    action = UpdatePurchases()
    assert datetime.timedelta(days=int(1)) == action.get_duration('foo_d1')
    assert datetime.timedelta(days=int(2) *
                              7) == action.get_duration('foo_bar_w2')
    assert dateutil.relativedelta.relativedelta(
        months=3) == action.get_duration('foo_m3_bar')
    assert dateutil.relativedelta.relativedelta(
        years=4) == action.get_duration('y4_foo_bar')
