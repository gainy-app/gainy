{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, category_id'),
      index('id', true),
      'delete from {{this}} where updated_at < (select max(updated_at) from {{this}})',
    ]
  )
}}


with
    common_stocks as (select * from {{ ref('tickers') }} where "type" ilike 'common stock'),

    /*( -- Cat: Crypto (add here when that cat would be ready) ) 
    *  no such category. For crypto we are using 2 components: interests and risk
    */
    simple_ticker_categories as
        (
            ( -- Cat: ETF
                select c.id       as category_id,
                       t.symbol,
                       1.0::float as sim_dif
                from {{ ref('tickers') }} t
                         join {{ ref('categories') }} c on c.name = 'ETF'
                where t."type" ilike 'ETF'
            )

            union

            ( -- Cat: Penny
                with
                    penny_sim_dif as
                        (
                            select c.id                                                 as category_id,
                                   t.symbol,
                                   -- right wing of bell-curve radial funcion (1/(1+x^2))|E(f):[0..1) (shifted down by 0.5 and scaled by x2 so E(f):(-1..1))
                                   -- with adjusted koef 1/10 so it gives 0.0 at price 10usd and adjusted pow 1.58496 for |x| so that for 5usd it gives 0.5
                                   -- since we are in 1-dimensional space for Penny (only price is the feature) it's just that one-sided bell-function
                                   2. / (1. + (abs(hpm.price_0d) / 10.) ^ 1.58496) - 1. as sim_dif
                            from common_stocks t
                                     join {{ ref('categories') }} c on c.name = 'Penny'
                                     join {{ ref('historical_prices_marked') }} hpm using (symbol)
                            where hpm.price_0d > 0 --no nulls or wrong prices (0,-r)
                        )

                select * from penny_sim_dif

            )

            union

            ( -- Cat: Defensive

                with
                    defensive_gicsctr as ( select * from common_stocks where gic_sector ilike any(array['Consumer Staples', 'Utilities', 'Health Care', 'Communication Services']) ),

                    downside_deviation_gicstcr AS
                        (
                            select defensive_gicsctr.gic_sector,
                                   percentile_cont(0.5) within group (order by downside_deviation asc) as median
                            from {{ ref('technicals') }}
                                     join defensive_gicsctr on defensive_gicsctr.symbol = technicals.symbol
                            where technicals.downside_deviation is not null
                            group by defensive_gicsctr.gic_sector
                        ),

                    defensive_cntrflip as
                        (
                            select t.symbol,
                                   -(t2.beta - 1) as beta, --center at threshold 1 and flip (<1 threshold from BusDoc)
                                   -(t2.downside_deviation - downside_deviation_gicstcr.median)/(1e-30 + downside_deviation_gicstcr.median) as downside_deviation --center at threshold 0 and flip, normalize per gic_sector
                            from defensive_gicsctr t
                                     join {{ ref('technicals') }} t2 on t.symbol = t2.symbol
                                     join downside_deviation_gicstcr on downside_deviation_gicstcr.gic_sector = t.gic_sector
                            where t2.beta is not null
                              and t2.downside_deviation is not null
                        ),

                    defensive_scalekoefs as
                        ( --scale koefs for pos and neg sides for good fit into D(CDF)=>sigmoid (D(f):[-7..7]=>E(f):[0..1]) (if it's kinda normal distribution - then it is symmetrical and ok, but usual itsn't, so left and right part need to have own scalings
                            --we take the point of medians amplitudes for pos and neg sides rspctvly and make that points to be the divisors-scale-koefs for input of CDF to make them equal to 1sigma, so that CDF(+/-1)=+/-0.75 (and tails of bigger amplitudes are squized))
                            --1e-30 epsilon to no ZeroDiv coz we will divide by this scale koef
                            select 1e-30 + coalesce((select percentile_cont(0.5) within group(order by beta asc) from defensive_cntrflip where beta > 0), 0.) as beta_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by downside_deviation asc) from defensive_cntrflip where downside_deviation > 0), 0.) as downside_deviation_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -beta asc) from defensive_cntrflip where beta < 0), 0.) as beta_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -downside_deviation asc) from defensive_cntrflip where downside_deviation < 0), 0.) as downside_deviation_k_d
                        ),

                    defensive_sim as
                        ( -- sim is a proximity to coord {1} in a positive and limited by CDF space{[0..1],[0..1],n}: (1.0-distance_L1avg({1;1;...;1},{x1;x2;...;xn}))
                            -- where distance_L1avg(A,B) = (|a1-b1|+|a2-b2|+...+|an-bn|)/n
                            -- and 1-(|1-b1|+|1-b2|..+|1-bn|)/n = 1-(1*n-b1-b2..-bn)/n = avg(B)
                            -- and coz of L1dist and positive and limited space:
                            --   1. similarity is symmetrical for coords {0} and {1}: sim_0 = 1-sim_1 so we can work with sim_1
                            --   2. sim_1=0.5 is the threshold point for conditions for ticker entering the category (avg(CDFn)=0.5)
                            -- we shift all sim_1 down by 0.5 and rescale by 2 to fit (-1..1) to use it as sim_dif wrt threshold conditions
                            select t.symbol,
                                   2.*(  pnorm(t.beta/(case when t.beta>0 then s.beta_k_u else s.beta_k_d end))
                                       + pnorm(t.downside_deviation/(case when t.downside_deviation>0 then s.downside_deviation_k_u else s.downside_deviation_k_d end))
                                       )/2. -1. as sim_dif
                            from defensive_cntrflip t -- no nulls in beta or dwnsidedev
                                     left join defensive_scalekoefs as s on true -- one row
                        ),

                    defensive_sim_scalekoefs as
                        ( --   and we will need to do additional step to refit from (-1..1) to [-1..1] for neg & pos sides again
                            --   (coz input n-dimnsnal coord combos and CDFs combos of them (as a consequence) can often be non-synchronous and no gurarantee for touching CDFs limits [0..1]
                            --   (e.g. lowest sim_1=0.105 ticker have {CDF1=0.01 but CDF2=0.2}=> and no any lower ticker with simultaneous {0.0,0.0} so we will need to rescale a bit there))
                            select 1e-30 + coalesce((select MAX(sim_dif) from defensive_sim where sim_dif > 0), 0.) as sim_dif_k_u,
                                   1e-30 + coalesce((select MAX(-sim_dif) from defensive_sim where sim_dif < 0), 0.) as sim_dif_k_d
                        ),

                    defensive_sim_dif as
                        (
                            select c.id 								                                         as category_id,
                                   t.symbol,
                                   t.sim_dif / (case when t.sim_dif>0 then s.sim_dif_k_u else s.sim_dif_k_d end) as sim_dif
                            from defensive_sim t
                                     join {{ ref('categories') }} c on c.name = 'Defensive'
                                     left join defensive_sim_scalekoefs as s on true -- one row
                        )

                select * from defensive_sim_dif

            )

            union

            ( -- Cat: Dividend

                with
                    last_consecutive_dividend_years as (
                        select symbol, sum(consecflagone) as cdy --,dy
                        from -- 3. sum up 1'nes-flags of consequtive years of dividending, starting from current or previous year back in time
                             (	select symbol, dy, -- gives 1 for consecutive numbers (desc, flag linked with prev row on current row), and first listed year always 1
                                         1+ max(dy) over(partition by symbol) - dy+1 - row_number() over (partition by symbol order by dy desc) as consecflagone
                                  from -- 2. make a list of symbol & dividend_year (dy)
                                       (	select distinct symbol, date_part('year', date)::int as dy
                                            from {{ ref('historical_dividends') }} d
                                                     -- 1. filter out only stocks that are dividending in current or previous year (dividends yearly or more frequently - catch both by cur_year minus one)
                                                     join (
                                                         select distinct symbol
                                                         from {{ ref('historical_dividends') }} d
                                                         where date >= date_trunc('year', now()) - interval '1 year'
                                                      ) as last_dy_thisorprev using (symbol)
                                       ) as symbol_dy
                             ) as symbol_dy_consec
                        where consecflagone = 1
                        group by symbol
                    ),

                    last_sixty_months_dividends as (
                        select symbol, sum(value) / 5 as avg_value_per_year
                        from {{ ref('historical_dividends') }}
                        where date > now() - interval '5 years'
                        group by symbol
                    ),

                    last_sixty_months_earnings as (
                        select eh.symbol, sum(eh.eps_actual) / 5 as avg_value_per_year
                        from {{ ref('earnings_history') }} eh
                        where eh.date::timestamp > now()::timestamp - interval '5 years'
                          and eh.eps_actual is not null
                        group by eh.symbol
                    ),

                    dividend_cntrflip as
                        (
                            select t.symbol,
                                   (coalesce(lcdy.cdy,0) - 5)::float as lcdy, -- center at threshold 5 years (BusDoc: Minimum 5 consecutive years of dividend payments)
                                   (tm.market_capitalization - 500000000) as markcap, -- center at threshold 500kk usd markcap (BusDoc: Minimum market capitalization of US$ 500 million)
                                   (h.dividend_share - lsmd.avg_value_per_year) as dpsdifavg, -- center at relative threshold (BusDoc: Dividend-per-share (DPS) greater than or equal to the five-year average dividend-per-share)
                                   (lsme.avg_value_per_year / lsmd.avg_value_per_year - 1.67) as dmedpsrat -- center at constant threshold (BusDoc: Average EPS/DPS ratio > 1.67 for the last 5 years)

                            from common_stocks t
                                     join {{ ref('ticker_metrics') }} tm using (symbol)
                                     join last_sixty_months_dividends lsmd using (symbol)
                                     join {{ ref('highlights') }} h using (symbol)
                                     join last_sixty_months_earnings lsme using (symbol)
                                     left join last_consecutive_dividend_years lcdy using (symbol) -- left join & coalesce 0, coz could lost some tickers with dps&dme that hasn't dividend on current or previous years, but has before (5 y horizon for dps dme) (got ~200 tickers 20220320)
                            where h.dividend_share is not null
                              and tm.market_capitalization is not null
                        ),

                    dividend_scalekoefs as
                        (
                            select 1e-30 + coalesce((select percentile_cont(0.5) within group(order by lcdy asc) from dividend_cntrflip where lcdy > 0), 0.) as lcdy_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by markcap asc) from dividend_cntrflip where markcap > 0), 0.) as markcap_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by dpsdifavg asc) from dividend_cntrflip where dpsdifavg > 0), 0.) as dpsdifavg_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by dmedpsrat asc) from dividend_cntrflip where dmedpsrat > 0), 0.) as dmedpsrat_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -lcdy asc) from dividend_cntrflip where lcdy < 0), 0.) as lcdy_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -markcap asc) from dividend_cntrflip where markcap < 0), 0.) as markcap_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -dpsdifavg asc) from dividend_cntrflip where dpsdifavg < 0), 0.) as dpsdifavg_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -dmedpsrat asc) from dividend_cntrflip where dmedpsrat < 0), 0.) as dmedpsrat_k_d
                        ),

                    dividend_sim as
                        (
                            select t.symbol,
                                   2.*(  pnorm(t.lcdy/(case when t.lcdy>0 then s.lcdy_k_u else s.lcdy_k_d end))
                                       + pnorm(t.markcap/(case when t.markcap>0 then s.markcap_k_u else s.markcap_k_d end))
                                       + pnorm(t.dpsdifavg/(case when t.dpsdifavg>0 then s.dpsdifavg_k_u else s.dpsdifavg_k_d end))
                                       + pnorm(t.dmedpsrat/(case when t.dmedpsrat>0 then s.dmedpsrat_k_u else s.dmedpsrat_k_d end))
                                       )/4. -1. as sim_dif
                            from dividend_cntrflip t -- no nulls
                                     left join dividend_scalekoefs as s on true -- one row
                        ),

                    dividend_sim_scalekoefs as
                        (
                            select 1e-30 + coalesce((select MAX(sim_dif) from dividend_sim where sim_dif > 0), 0.) as sim_dif_k_u,
                                   1e-30 + coalesce((select MAX(-sim_dif) from dividend_sim where sim_dif < 0), 0.) as sim_dif_k_d
                        ),

                    dividend_sim_dif as
                        (
                            select c.id                                                                          as category_id,
                                   t.symbol,
                                   t.sim_dif / (case when t.sim_dif>0 then s.sim_dif_k_u else s.sim_dif_k_d end) as sim_dif
                            from dividend_sim t
                                     join {{ ref('categories') }} c on c.name = 'Dividend'
                                     left join dividend_sim_scalekoefs as s on true -- one row
                        )

                select * from dividend_sim_dif

            )

            union

            ( -- Cat: Momentum

                with
                    momentum_cntrflip as
                        (
                            select symbol,
                                   combined_momentum_score --center at threshold 0
                            from common_stocks t
                                     join {{ ref('ticker_momentum_metrics') }} using (symbol)
                            where combined_momentum_score is not null
                        ),

                    momentum_scalekoefs as
                        (
                            select 1e-30 + coalesce((select percentile_cont(0.5) within group(order by combined_momentum_score asc) from momentum_cntrflip where combined_momentum_score > 0), 0.) as combined_momentum_score_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -combined_momentum_score asc) from momentum_cntrflip where combined_momentum_score < 0), 0.) as combined_momentum_score_k_d
                        ),

                    momentum_sim as
                        (
                            select t.symbol,
                                   2.*(  pnorm(t.combined_momentum_score/(case when t.combined_momentum_score>0 then s.combined_momentum_score_k_u else s.combined_momentum_score_k_d end))
                                       ) -1. as sim_dif
                            from momentum_cntrflip t -- no nulls in combined_momentum_score
                                     left join momentum_scalekoefs as s on true -- one row
                        ),

                    momentum_sim_scalekoefs as
                        (
                            select 1e-30 + coalesce((select MAX(sim_dif) from momentum_sim where sim_dif > 0), 0.) as sim_dif_k_u,
                                   1e-30 + coalesce((select MAX(-sim_dif) from momentum_sim where sim_dif < 0), 0.) as sim_dif_k_d
                        ),

                    momentum_sim_dif as
                        (
                            select c.id                                                                          as category_id,
                                   t.symbol,
                                   t.sim_dif / (case when t.sim_dif>0 then s.sim_dif_k_u else s.sim_dif_k_d end) as sim_dif
                            from momentum_sim t
                                     join {{ ref('categories') }} c on c.name = 'Momentum'
                                     left join momentum_sim_scalekoefs as s on true -- one row
                        )

                select * from momentum_sim_dif

            )

            union

            ( -- Cat: Value (it's reverse to Growth by rules..)

                with
                    value_cntrflip as
                        (
                            select symbol,
                                   value_score, --center at threshold 0
                                   -growth_score as growth_score --center at threshold 0 and flip
                            from common_stocks t
                                     join {{ ref('ticker_value_growth_metrics') }} using (symbol)
                            where value_score is not null
                              and growth_score is not null
                        ),

                    value_scalekoefs as
                        (
                            select 1e-30 + coalesce((select percentile_cont(0.5) within group(order by value_score asc) from value_cntrflip where value_score > 0), 0.) as value_score_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by growth_score asc) from value_cntrflip where growth_score > 0), 0.) as growth_score_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -value_score asc) from value_cntrflip where value_score < 0), 0.) as value_score_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -growth_score asc) from value_cntrflip where growth_score < 0), 0.) as growth_score_k_d
                        ),

                    value_sim as
                        (
                            select t.symbol,
                                   2.*(  pnorm(t.value_score/(case when t.value_score>0 then s.value_score_k_u else s.value_score_k_d end))
                                       + pnorm(t.growth_score/(case when t.growth_score>0 then s.growth_score_k_u else s.growth_score_k_d end))
                                       )/2. -1. as sim_dif
                            from value_cntrflip t -- no nulls
                                     left join value_scalekoefs as s on true -- one row
                        ),

                    value_sim_scalekoefs as
                        (
                            select 1e-30 + coalesce((select MAX(sim_dif) from value_sim where sim_dif > 0), 0.) as sim_dif_k_u,
                                   1e-30 + coalesce((select MAX(-sim_dif) from value_sim where sim_dif < 0), 0.) as sim_dif_k_d
                        ),

                    value_sim_dif as
                        (
                            select c.id                                                                          as category_id,
                                   t.symbol,
                                   t.sim_dif / (case when t.sim_dif>0 then s.sim_dif_k_u else s.sim_dif_k_d end) as sim_dif
                            from value_sim t
                                     join {{ ref('categories') }} c on c.name = 'Value'
                                     left join value_sim_scalekoefs as s on true -- one row
                        )

                select * from value_sim_dif

            )

            union

            ( -- Cat: Growth (it's reverse to Value by rules..)

                with
                    growth_cntrflip as
                        (
                            select t.symbol,
                                   -value_score as value_score, --center at threshold 0 and flip
                                   growth_score --center at threshold 0
                            from common_stocks t
                                     join {{ ref('ticker_value_growth_metrics') }} using (symbol)
                            where value_score is not null
                              and growth_score is not null
                        ),

                    growth_scalekoefs as
                        (
                            select 1e-30 + coalesce((select percentile_cont(0.5) within group(order by value_score asc) from growth_cntrflip where value_score > 0), 0.) as value_score_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by growth_score asc) from growth_cntrflip where growth_score > 0), 0.) as growth_score_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -value_score asc) from growth_cntrflip where value_score < 0), 0.) as value_score_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -growth_score asc) from growth_cntrflip where growth_score < 0), 0.) as growth_score_k_d
                        ),

                    growth_sim as
                        (
                            select t.symbol,
                                   2.*(  pnorm(t.value_score/(case when t.value_score>0 then s.value_score_k_u else s.value_score_k_d end))
                                       + pnorm(t.growth_score/(case when t.growth_score>0 then s.growth_score_k_u else s.growth_score_k_d end))
                                       )/2. -1. as sim_dif
                            from growth_cntrflip t -- no nulls
                                     left join growth_scalekoefs as s on true -- one row
                        ),

                    growth_sim_scalekoefs as
                        (
                            select 1e-30 + coalesce((select MAX(sim_dif) from growth_sim where sim_dif > 0), 0.) as sim_dif_k_u,
                                   1e-30 + coalesce((select MAX(-sim_dif) from growth_sim where sim_dif < 0), 0.) as sim_dif_k_d
                        ),

                    growth_sim_dif as
                        (
                            select c.id                                                                          as category_id,
                                   t.symbol,
                                   t.sim_dif / (case when t.sim_dif>0 then s.sim_dif_k_u else s.sim_dif_k_d end) as sim_dif
                            from growth_sim t
                                     join {{ ref('categories') }} c on c.name = 'Growth'
                                     left join growth_sim_scalekoefs as s on true -- one row
                        )

                select * from growth_sim_dif

            )

            union

            ( -- Cat: Speculation (or or or case)

                with
                    max_eps as
                        (
                            select t.symbol,
                                   MAX(eh.eps_actual) as value
                            from {{ ref('tickers') }} t
                                     join {{ ref('earnings_history') }} eh on t.symbol = eh.symbol
                            where t.type != 'crypto'
                            group by t.symbol
                        ),
                    options_stats as
                        (
                            select code,
                                   SUM(callopeninterest)*100  as call_option_shares_deliverable_outstanding  --BusDoc: Call option shares deliverable outstanding exceeds total shares outstanding (OPEN INTEREST = actual options quantity on the market, we focused on Call type)
                                   --SUM(callvolume) * 100 as call_option_shares_deliverable_outstanding -- bad idea, it's just transactions counter, non uniq options
                            from {{ source('eod', 'eod_options') }}
                            where expirationdate::timestamp > now()::timestamp
                            group by code
                        ),

                    specul_cntrflip as
                        ( -- we have greatest(OR cond OR cond OR cond)-condition-rule for Specul category, that is mean we need at least 1 non-null cond in condition rule for any ticker to do the soft-measure,
                            -- so do the left join here and we have that special rule for pre-revenue company: if eps is null then ticker just on the threshold line so we coalesce eps=null to 0.
                            select t.symbol,
                                   (t2.beta-3.)       						as beta,      --BusDoc: 3x volatility of QQQ (we don't know for sure how EOD calc industry/market basis for beta.. maybe it's index qqq)
                                   -coalesce(max_eps.value,1e-30)			as maxeps,    --BusDoc: Pre-revenue stock (earn per share zero or negative or unknown(eq0 still waiting for good product from company))
                                   options_stats.call_option_shares_deliverable_outstanding
                                   - ticker_shares_stats.shares_outstanding as opt_vs_shr --BusDoc: Call option shares deliverable outstanding exceeds total shares outstanding
                            from common_stocks t
                                     left join {{ ref('technicals') }} t2 using (symbol)
                                     left join {{ ref('ticker_shares_stats') }} using (symbol)
                                     left join max_eps using (symbol)
                                     left join options_stats on options_stats.code = t.symbol
                        ),

                    specul_scalekoefs as
                        (
                            select 1e-30 + coalesce((select percentile_cont(0.5) within group(order by beta asc) from specul_cntrflip where beta > 0), 0.) as beta_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by maxeps asc) from specul_cntrflip where maxeps > 0), 0.) as maxeps_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by opt_vs_shr asc) from specul_cntrflip where opt_vs_shr > 0), 0.) as opt_vs_shr_k_u,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -beta asc) from specul_cntrflip where beta < 0), 0.) as beta_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -maxeps asc) from specul_cntrflip where maxeps < 0), 0.) as maxeps_k_d,
                                   1e-30 + coalesce((select percentile_cont(0.5) within group(order by -opt_vs_shr asc) from specul_cntrflip where opt_vs_shr < 0), 0.) as opt_vs_shr_k_d
                        ),

                    specul_sim as
                        (
                            select t.symbol,
                                   2.*greatest(pnorm(t.beta/(case when t.beta>0 then s.beta_k_u else s.beta_k_d end)),
                                               pnorm(t.maxeps/(case when t.maxeps>0 then s.maxeps_k_u else s.maxeps_k_d end)),
                                               pnorm(t.opt_vs_shr/(case when t.opt_vs_shr>0 then s.opt_vs_shr_k_u else s.opt_vs_shr_k_d end))
                                       ) -1. as sim_dif
                            from specul_cntrflip t -- guaranteed no nulls in max_eps coz of coealesce(max_eps.value,0.) in specul_cntrflip so greatest() always gives at least 0. as value
                                     left join specul_scalekoefs as s on true -- one row
                        ),

                    specul_sim_scalekoefs as
                        (
                            select 1e-30 + coalesce((select MAX(sim_dif) from specul_sim where sim_dif > 0), 0.) as sim_dif_k_u,
                                   1e-30 + coalesce((select MAX(-sim_dif) from specul_sim where sim_dif < 0), 0.) as sim_dif_k_d
                        ),

                    specul_sim_dif as
                        (
                            select c.id                                                                          as category_id,
                                   t.symbol,
                                   t.sim_dif / (case when t.sim_dif>0 then s.sim_dif_k_u else s.sim_dif_k_d end) as sim_dif
                            from specul_sim t
                                     join {{ ref('categories') }} c on c.name = 'Speculation'
                                     left join specul_sim_scalekoefs as s on true -- one row
                        )

                select * from specul_sim_dif

            )
        ),
    complex_ticker_categories as
        (
            select simple_ticker_categories.category_id,
                   ticker_components_flat.symbol,
                   sum(simple_ticker_categories.sim_dif * component_weight) as sim_dif,
                   sum(component_weight)                                    as component_weight_sum
            from {{ ref('ticker_components_flat') }}
                     join simple_ticker_categories
                          on simple_ticker_categories.symbol = ticker_components_flat.component_symbol
            group by simple_ticker_categories.category_id, ticker_components_flat.symbol
         ),
    complex_ticker_categories_stats as
        (
            select symbol,
                   sum(sim_dif)              as sim_dif_sum,
                   sum(component_weight_sum) as component_weight_sum
            from complex_ticker_categories
            group by symbol
        ),
    complex_ticker_categories_normalized as
        (
            select category_id,
                   symbol,
                   sim_dif / complex_ticker_categories_stats.component_weight_sum as sim_dif
            from complex_ticker_categories
                     join complex_ticker_categories_stats using (symbol)
            where complex_ticker_categories_stats.component_weight_sum > 0
        )

select (simple_ticker_categories.category_id || '_' || symbol)                                       as id,
       simple_ticker_categories.category_id,
       symbol,
       simple_ticker_categories.sim_dif,
       now()::timestamp                                                                              as updated_at,
       (row_number() over (partition by symbol order by simple_ticker_categories.sim_dif desc))::int as rank
from simple_ticker_categories
         left join complex_ticker_categories using (symbol)
where complex_ticker_categories.symbol is null

union all

select (category_id || '_' || symbol)                                       as id,
       category_id,
       symbol,
       sim_dif,
       now()::timestamp                                                     as updated_at,
       (row_number() over (partition by symbol order by sim_dif desc))::int as rank
from complex_ticker_categories