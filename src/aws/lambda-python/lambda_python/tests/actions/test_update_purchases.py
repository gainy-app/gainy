import os, sys
import datetime

task_dir = os.path.abspath(os.path.join(__file__, '../../../'))
sys.path.append(task_dir)

from actions import UpdatePurchases


def test_get_duration():
    action = UpdatePurchases()
    assert datetime.timedelta(days=1) == action.get_duration('foo_d1')
    assert datetime.timedelta(days=2 * 7) == action.get_duration('foo_bar_w2')
    assert datetime.timedelta(days=3 * 30) == action.get_duration('foo_m3_bar')
    assert datetime.timedelta(days=4 *
                              365) == action.get_duration('y4_foo_bar')
