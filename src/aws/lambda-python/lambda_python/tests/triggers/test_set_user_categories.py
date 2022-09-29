import os, sys

import pytest

task_dir = os.path.abspath(os.path.join(__file__, '../../../'))
sys.path.append(task_dir)

from triggers import SetUserCategories


def get_test_data():
    return [
        ({
            "profile_id": 22183,
            "risk_level": 0.23200755,
            "average_market_return": 6,
            "investment_horizon": 0.56439394,
            "unexpected_purchases_source": "checking_savings",
            "damage_of_failure": 0.28314394,
            "stock_market_risk_level": "somewhat_risky",
            "trading_experience": "etfs_and_safe_stocks",
            "if_market_drops_20_i_will_buy": 0.5,
            "if_market_drops_40_i_will_buy": 0.5,
        }, 2),
        ({
            "profile_id": 22225,
            "risk_level": 0.5104166,
            "average_market_return": 6,
            "investment_horizon": 0.62689394,
            "unexpected_purchases_source": "other_loans",
            "damage_of_failure": 0.45549244,
            "stock_market_risk_level": "somewhat_risky",
            "trading_experience": "etfs_and_safe_stocks",
            "if_market_drops_20_i_will_buy": 0.5,
            "if_market_drops_40_i_will_buy": 0.5,
        }, 2),
        ({
            "profile_id": 22492,
            "risk_level": 0.4985207,
            "average_market_return": 15,
            "investment_horizon": 1,
            "unexpected_purchases_source": "checking_savings",
            "damage_of_failure": 0.50147927,
            "stock_market_risk_level": "neutral",
            "trading_experience": "very_little",
            "if_market_drops_20_i_will_buy": 0.5,
            "if_market_drops_40_i_will_buy": 0.5,
        }, 2),
    ]


@pytest.mark.parametrize("payload,expected_score", get_test_data())
def test(payload, expected_score):
    trigger = SetUserCategories()
    assert expected_score == trigger.calculate_score(payload)
