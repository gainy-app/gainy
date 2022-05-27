import csv
import json
import os
import sys
from math import trunc
from psycopg2.extras import execute_values
from common.hasura_function import HasuraTrigger
from service.logging import get_logger

logger = get_logger(__name__)
script_directory = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_directory)

with open(
        os.path.join(
            script_directory,
            '../data/user_categories_decision_matrix.csv')) as csv_file:
    reader = csv.DictReader(csv_file, delimiter='\t')
    decision_matrix = list(reader)


class SetUserCategories(HasuraTrigger):

    def __init__(self):
        super().__init__("set_user_categories")

    def get_allowed_profile_ids(self, op, data):
        return data["new"]['profile_id']

    def apply(self, db_conn, op, data):
        payload = self._extract_payload(data)

        risk_needed = [1, 2, 2, 3][self._list_index(payload['risk_level'], 4)]
        if payload['average_market_return'] == 6 and risk_needed > 1:
            risk_needed = 3
        if payload['average_market_return'] == 50 and risk_needed < 3:
            risk_needed = 2

        investment_horizon_points = [1, 1, 2, 3][self._list_index(
            payload['investment_horizon'], 4)]
        unexpected_purchases_source_points = {
            'checking_savings': 3,
            'stock_investments': 2,
            'credit_card': 1,
            'other_loans': 1
        }[payload['unexpected_purchases_source']]
        damage_of_failure_points = [1, 2, 2, 3][self._list_index(
            payload['damage_of_failure'], 4)]
        risk_taking_ability = round(
            (investment_horizon_points + unexpected_purchases_source_points +
             damage_of_failure_points) / 3)

        stock_market_risk_level_points = {
            'very_risky': 1,
            'somewhat_risky': 2,
            'neutral': 2,
            'somewhat_safe': 3,
            'very_safe': 3,
        }[payload['stock_market_risk_level']]
        trading_experience_points = {
            'never_tried': 2,
            'very_little': 2,
            'companies_i_believe_in': 2,
            'etfs_and_safe_stocks': 2,
            'advanced': 3,
            'daily_trader': 3,
            'investment_funds': 2,
            'professional': 3,
            'dont_trade_after_bad_experience': 1
        }[payload['trading_experience']]

        loss_tolerance = round(
            (stock_market_risk_level_points + trading_experience_points) / 2)

        for i in [
                'if_market_drops_20_i_will_buy',
                'if_market_drops_40_i_will_buy'
        ]:
            if payload[i] is not None:
                buy_rate = payload[i] * 3
                if buy_rate < 1 and loss_tolerance == 3:  # sell
                    loss_tolerance -= 1
                if buy_rate > 2 and loss_tolerance != 3:  # buy
                    loss_tolerance += 1

        final_score = max(risk_needed, risk_taking_ability, loss_tolerance)
        for i in decision_matrix:
            if i['Risk Need'] == risk_needed and i[
                    'Risk Taking Ability'] == risk_taking_ability and i[
                        'Loss Tolerance'] == loss_tolerance:
                final_score = i['Hard code matrix']

        profile_id = payload["profile_id"]
        with db_conn.cursor() as cursor:
            cursor.execute(
                "update app.profile_scoring_settings set risk_score = %(risk_score)s where profile_id = %(profile_id)s",
                {
                    'risk_score': final_score,
                    "profile_id": profile_id
                })

            cursor.execute(
                "select id from categories where risk_score = %(risk_score)s",
                {'risk_score': final_score})

            rows = cursor.fetchall()
            categories = [row[0] for row in rows]

        logger.info('set_user_categories ' +
                    json.dumps({
                        'profile_id': profile_id,
                        'risk_needed': risk_needed,
                        'risk_taking_ability': risk_taking_ability,
                        'loss_tolerance': loss_tolerance,
                        'final_score': final_score,
                        'categories': categories,
                    }))

        with db_conn.cursor() as cursor:
            cursor.execute(
                "delete from app.profile_categories where profile_id = %(profile_id)s",
                {'profile_id': profile_id})

            execute_values(
                cursor,
                "INSERT INTO app.profile_categories (profile_id, category_id) VALUES %s",
                [(profile_id, category_id) for category_id in categories])

    @staticmethod
    def _list_index(value, list_size):
        """
        Select the list index between 0 and `list_size` - 1 based on ``value`` parameter
        :param value: real number between 0 and 1
        :param list_size: the size of the list
        :return: the index between 0 and `list_size` - 1
        """
        if value >= 1.0:
            # covers case where `value` = 1.0
            return list_size - 1

        return trunc(value * list_size)
