INSERT INTO app.profiles (id, email, first_name, last_name, gender, created_at, user_id, avatar_url,
                                    legal_address)
             VALUES (1, 'test-demo-portfolio@gainy.app', 'fn', 'ln', 0, '2021-10-18 11:46:18.851570 +00:00',
                     'AO0OQyz0jyL5lNUpvKbpVdAPvlI3', '',
                     'legal_address'),
                    (2, 'test-ttf-portfolio@gainy.app', 'fn', 'ln', 0, '2021-10-18 11:46:18.851570 +00:00',
                     'AO0OQyz0jyL5lNUpvKbpVdAPvlI4', '',
                     'legal_address'),
                    (3, 'test-plaid-portfolio@gainy.app', 'fn', 'ln', 0, '2021-10-18 11:46:18.851570 +00:00',
                     'AO0OQyz0jyL5lNUpvKbpVdAPvlI5', '',
                     'legal_address');
ALTER SEQUENCE app.profiles_id_seq RESTART WITH 4;

INSERT INTO app.profile_scoring_settings (profile_id, created_at, risk_level, average_market_return, investment_horizon,
                                   unexpected_purchases_source, damage_of_failure, stock_market_risk_level,
                                   trading_experience, if_market_drops_20_i_will_buy,
                                   if_market_drops_40_i_will_buy)
VALUES (1, '2021-10-18 11:46:18.851570 +00:00', 0.5, 6, 0.5, 'checking_savings', 0.5, 'very_risky', 'never_tried', 0.5, 0.5),
       (2, '2021-10-18 11:46:18.851570 +00:00', 0.5, 6, 0.5, 'checking_savings', 0.5, 'very_risky', 'never_tried', 0.5, 0.5);
