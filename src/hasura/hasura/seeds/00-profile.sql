with inserted_profile as
         (INSERT INTO app.profiles (email, first_name, last_name, gender, created_at, user_id, avatar_url,
                                    legal_address)
             VALUES ('test@example.com', 'fn', 'ln', 0, '2021-10-18 11:46:18.851570 +00:00',
                     'AO0OQyz0jyL5lNUpvKbpVdAPvlI2', '',
                     'legal_address')
             RETURNING id)
INSERT
INTO app.profile_scoring_settings (profile_id, created_at, risk_level, average_market_return, investment_horizon,
                                   unexpected_purchases_source, damage_of_failure, stock_market_risk_level,
                                   trading_experience, if_market_drops_20_i_will_buy,
                                   if_market_drops_40_i_will_buy)
VALUES ((SELECT id FROM inserted_profile), '2021-10-18 11:46:18.851570 +00:00', 0.5, 6, 0.5, 'checking_savings', 0.5,
        'very_risky',
        'never_tried', 0.5,
        0.5)
