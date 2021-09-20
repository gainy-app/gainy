import os
import sys

script_directory = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_directory)

import json
import csv
import psycopg2
from psycopg2.extras import execute_values
from common import hasura_response

host = os.environ['pg_host']
port = os.environ['pg_port']
dbname = os.environ['pg_dbname']
username = os.environ['pg_username']
password = os.environ['pg_password']

with open(
        os.path.join(script_directory,
                     'data/user_categories_decision_matrix.csv')) as csv_file:
    reader = csv.DictReader(csv_file, delimiter='\t')
    decision_matrix = list(reader)


def handle(event, context):
    print(json.dumps(event))
    try:
        body = json.loads(event['body'])
        hasura_user_id = body['event']['session_variables']['x-hasura-user-id']
    except:
        return hasura_response.unauthorized()

    print(hasura_user_id)

    with psycopg2.connect("host=%s port=%s dbname=%s user=%s password=%s" %
                          (host, port, dbname, username, password)) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """select pss.*
                            from app.profile_scoring_settings pss
                                     join app.profiles p ON pss.profile_id = p.id
                            where p.user_id = %(hasura_user_id)s""",
                {'hasura_user_id': hasura_user_id})
            columns = [column[0] for column in cursor.description]
            row = cursor.fetchone()

        if row is None:
            return hasura_response.bad_request('No onboarding data available')

        data = dict(zip(columns, row))
        profile_id = data['profile_id']

        risk_needed = [1, 2, 2, 3][round(data['risk_level'] * 4)]
        if data['average_market_return'] == 6 and risk_needed > 1:
            risk_needed = 3
        if data['average_market_return'] == 50 and risk_needed < 3:
            risk_needed = 2

        investment_horizon_points = [1, 1, 2,
                                     3][round(data['investment_horizon'] * 4)]
        unexpected_purchases_source_points = {
            'checking_savings': 3,
            'stock_investments': 2,
            'credit_card': 1,
            'other_loans': 1
        }[data['unexpected_purchases_source']]
        damage_of_failure_points = [1, 2, 2,
                                    3][round(data['damage_of_failure'] * 4)]
        risk_taking_ability = round(
            (investment_horizon_points + unexpected_purchases_source_points +
             damage_of_failure_points) / 3)

        stock_market_risk_level_points = {
            'very_risky': 1,
            'somewhat_risky': 2,
            'neutral': 2,
            'somewhat_safe': 3,
            'very_safe': 3,
        }[data['stock_market_risk_level']]
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
        }[data['trading_experience']]

        loss_tolerance = round(
            (stock_market_risk_level_points + trading_experience_points) / 2)

        for i in [
                'if_market_drops_20_i_will_buy',
                'if_market_drops_40_i_will_buy'
        ]:
            if data[i] is not None:
                buy_rate = data[i] * 3
                if buy_rate < 1 and loss_tolerance == 3:  #sell
                    loss_tolerance -= 1
                if buy_rate > 2 and loss_tolerance != 3:  #buy
                    loss_tolerance += 1

        final_score = max(risk_needed, risk_taking_ability, loss_tolerance)
        for i in decision_matrix:
            if i['Risk Need'] == risk_needed and i[
                    'Risk Taking Ability'] == risk_taking_ability and i[
                        'Loss Tolerance'] == loss_tolerance:
                final_score = i['Hard code matrix']

        with conn.cursor() as cursor:
            cursor.execute(
                "select id from public.categories where risk_score = %(risk_score)s",
                {'risk_score': final_score})

            rows = cursor.fetchall()
            print(rows)
            categories = [row[0] for row in rows]

        print({
            'hasura_user_id': hasura_user_id,
            'risk_needed': risk_needed,
            'risk_taking_ability': risk_taking_ability,
            'loss_tolerance': loss_tolerance,
            'final_score': final_score,
            'categories': categories,
        })

        with conn.cursor() as cursor:
            execute_values(
                cursor,
                "INSERT INTO app.profile_categories (profile_id, category_id) VALUES %s",
                [(profile_id, category_id) for category_id in categories])

        return hasura_response.success(None)
