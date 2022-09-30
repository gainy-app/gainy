alter table "app"."profile_scoring_settings" add column "risk_score" integer
 null;

with user_categories_decision_matrix as
         (
             select * from ( values
                                 (3,3,3,3,3),
                                 (2,3,3,3,3),
                                 (1,3,3,3,3),
                                 (3,2,2,null,2),
                                 (2,2,2,2,2),
                                 (1,2,2,2,2),
                                 (3,1,1,null,1),
                                 (2,1,1,null,2),
                                 (1,1,1,1,1),
                                 (3,3,2,null,2),
                                 (2,3,2,2,2),
                                 (1,3,2,2,2),
                                 (3,3,1,null,2),
                                 (2,3,1,null,2),
                                 (1,3,1,1,1),
                                 (3,2,1,null,2),
                                 (2,2,1,null,2),
                                 (1,2,1,1,1),
                                 (3,2,3,2,2),
                                 (2,2,3,2,2),
                                 (1,2,3,2,2),
                                 (3,1,3,1,1),
                                 (2,1,3,1,1),
                                 (1,1,3,1,1),
                                 (3,1,2,1,3),
                                 (2,1,2,1,1),
                                 (1,1,2,1,1)
                           ) t ("risk_needed", "risk_taking_ability", "loss_tolerance", "Decision matrix code", "Hard code matrix")
         ),
     scoring_settings0 as
         (
             select case
                        when average_market_return = 6 and risk_level > 0.25
                            then 3
                        when average_market_return = 50 and risk_level < 0.75
                            then 2
                        else (ARRAY [1, 2, 2, 3])[floor(least(1 + risk_level * 4, 4))]
                        end                                                           as risk_needed,
                    (ARRAY [1, 1, 2, 3])[floor(least(1 + investment_horizon * 4, 4))] as investment_horizon_points,
                    (ARRAY [1, 2, 2, 3])[floor(least(1 + damage_of_failure * 4, 4))]  as damage_of_failure_points,
                    case
                        when unexpected_purchases_source = 'checking_savings' then 3
                        when unexpected_purchases_source = 'stock_investments' then 2
                        when unexpected_purchases_source = 'credit_card' then 1
                        when unexpected_purchases_source = 'other_loans' then 1
                        end                                                           as unexpected_purchases_source_points,
                    case
                        when stock_market_risk_level = 'very_risky' then 1
                        when stock_market_risk_level = 'somewhat_risky' then 2
                        when stock_market_risk_level = 'neutral' then 2
                        when stock_market_risk_level = 'somewhat_safe' then 3
                        when stock_market_risk_level = 'very_safe' then 3
                        end                                                           as stock_market_risk_level_points,
                    case
                        when trading_experience = 'never_tried' then 2
                        when trading_experience = 'very_little' then 2
                        when trading_experience = 'companies_i_believe_in' then 2
                        when trading_experience = 'etfs_and_safe_stocks' then 2
                        when trading_experience = 'advanced' then 3
                        when trading_experience = 'daily_trader' then 3
                        when trading_experience = 'investment_funds' then 2
                        when trading_experience = 'professional' then 3
                        when trading_experience = 'dont_trade_after_bad_experience' then 1
                        end                                                           as trading_experience_points,
                    if_market_drops_20_i_will_buy,
                    if_market_drops_40_i_will_buy,
                    profile_id
             from app.profile_scoring_settings
         ),
     scoring_settings1 as
         (
             select profile_id,
                    risk_needed,
                    round((investment_horizon_points + unexpected_purchases_source_points + damage_of_failure_points)::double precision /
                          3) as                                                             risk_taking_ability,

                    round((stock_market_risk_level_points + trading_experience_points) / 2) loss_tolerance,
                    if_market_drops_20_i_will_buy,
                    if_market_drops_40_i_will_buy

             from scoring_settings0
         ),
     scoring_settings2 as
         (
             select profile_id,
                    risk_needed,
                    risk_taking_ability,
                    case
                        when if_market_drops_20_i_will_buy * 3 < 1 and loss_tolerance = 3
                            then loss_tolerance - 1
                        when if_market_drops_20_i_will_buy * 3 > 2 and loss_tolerance != 3
                            then loss_tolerance + 1
                        else loss_tolerance
                        end as loss_tolerance,
                    if_market_drops_40_i_will_buy
             from scoring_settings1
         ),
     scoring_settings3 as
         (
             select profile_id,
                    risk_needed,
                    risk_taking_ability,
                    case
                        when if_market_drops_40_i_will_buy * 3 < 1 and loss_tolerance = 3
                            then loss_tolerance - 1
                        when if_market_drops_40_i_will_buy * 3 > 2 and loss_tolerance != 3
                            then loss_tolerance + 1
                        else loss_tolerance
                        end as loss_tolerance
             from scoring_settings2
         ),
     scoring_settings4 as
         (
             select profile_id,
                    coalesce("Hard code matrix",
                             greatest(risk_needed, risk_taking_ability, loss_tolerance)) as final_score
             from scoring_settings3
                      left join user_categories_decision_matrix using (risk_needed, risk_taking_ability, loss_tolerance)
         )
update app.profile_scoring_settings
set risk_score = scoring_settings4.final_score
from scoring_settings4
where profile_scoring_settings.profile_id = scoring_settings4.profile_id and risk_score is null;
