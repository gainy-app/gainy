{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with ticker_risk_score_common_stocks as
         (
             with ticker_riskscore_categorial_weights_frominvestcats as
                      (
                          select tcc.symbol,
                                 cat.risk_score               as risk_category,
                                 (1. + max(tcc.sim_dif)) / 2. as weight
                          from {{ ref('ticker_categories_continuous') }} tcc
                                   join {{ ref('categories') }} cat
                          on cat.id = tcc.category_id
                          group by tcc.symbol, cat.risk_score
                      ),

                  ticker_riskscore_categorial_weights_fromvolatility as
                      (
                          with ticker_volatility_nrmlzd as
                                   (
                                       select symbol,
                                              (tm.absolute_historical_volatility_adjusted_current - maxmin.v_min) /
                                              (1e-30 + (maxmin.v_max - maxmin.v_min)) as val
                                       from {{ ref('ticker_metrics') }} tm
                                                left join (
                                                    select max(absolute_historical_volatility_adjusted_current) as v_max,
                                                           min(absolute_historical_volatility_adjusted_current) as v_min
                                                    from {{ ref('ticker_metrics') }} tm
                                                    where tm.absolute_historical_volatility_adjusted_current is not null
                                                    ) as maxmin
                                       on true
                                       where tm.absolute_historical_volatility_adjusted_current is not null
                                   )
                               -- using parameterized bell function - place 3 sensory bells to measure proximity for each risk category (1,2,3) in one dimension
                               -- latex: \frac{1}{1+\left(s_{r}+\left(s_{c}-s_{r}\right)\cdot\frac{\left|a-0.5\right|}{0.5}\right)^{d}\cdot\left|a-x\right|^{d}},\ s_{r}=8.3,\ s_{c}=4,\ d=3.8,\ s_{c}\le s_{r},\ 0.\le a\le1.
                               -- desmos: https://www.desmos.com/calculator/olnm6cthlt ("a" stands for the coord, so move it around and look at black graph - it's the sensor region)
                              (
                                  select symbol, -- risk = 1, bell-sensor coord={0.0}
                                         1::int                                                 as risk_category,
                                         1. / (1. + abs(0.0 - tvn.val) ^ 3.8
                                             * (8.3 + (4. - 8.3) * abs(0.0 - 0.5) / 0.5) ^ 3.8) as weight
                                  from ticker_volatility_nrmlzd tvn
                              )
                          union
                          (
                              select symbol, -- risk = 2 , bell-sensor coord={0.5}
                                     2::int                                                 as risk_category,
                                     1. / (1. + abs(0.5 - tvn.val) ^ 3.8
                                         * (8.3 + (4. - 8.3) * abs(0.5 - 0.5) / 0.5) ^ 3.8) as weight
                              from ticker_volatility_nrmlzd tvn
                          )
                          union
                          (
                              select symbol, -- risk = 3 , bell-sensor coord={1.0}
                                     3::int                                                 as risk_category,
                                     1. / (1. + abs(1.0 - tvn.val) ^ 3.8
                                         * (8.3 + (4. - 8.3) * abs(1.0 - 0.5) / 0.5) ^ 3.8) as weight
                              from ticker_volatility_nrmlzd tvn
                          )
                      ),

                  ticker_riskscore_categorial_weights as
                      (
                          select -- in case when we don't have enought risk information from ours invest.categories - we mix with volatility information
                                 coalesce(trsc.symbol, trsv.symbol)               as symbol,
                                 coalesce(trsc.risk_category, trsv.risk_category) as risk_category,
                                 case
                                     when coalesce(trsc.weight, 0.) = 0
                                         then trsv.weight
                                     else trsc.weight
                                     end                                          as weight
                          from ticker_riskscore_categorial_weights_frominvestcats trsc
                                   full outer join ticker_riskscore_categorial_weights_fromvolatility trsv
                                                   on trsv.symbol = trsc.symbol and trsv.risk_category = trsc.risk_category
                      ),


                  ticker_riskscore_onedimensional_weighted as
                      (
                          select trcw.symbol,
                                 case
                                     when sum(trcw.weight) = 0
                                         then 0.0 -- in probably non-existing case if all weights was 0 (0 volatility of adjusted_close from eod's "historical_prices")
                                     else sum(trcw.risk_category * trcw.weight) / (1e-30 + sum(trcw.weight)) - 2. -- [1..3]=>(-1..1)
                                     end as risk
                          from ticker_riskscore_categorial_weights trcw
                          group by trcw.symbol
                      ),

                  -- 0=centered medium risk. but because we were used weighting and mixing we don't effectively touch the negative and positive limits [-1] and [+1]
                  -- but we need touch the limits in full scale [-1..1] - to interpret the lowest possible risk ticker and highest possible
                  -- so we now need to renorm negative and positive sides to touch the limits

                  scalekoefs as
                      (
                          select 1e-30 + coalesce((
                                                      select MAX(risk)
                                                      from ticker_riskscore_onedimensional_weighted
                                                      where risk > 0
                                                  ), 0.) as risk_k_u,
                                 1e-30 + coalesce((
                                                      select MAX(-risk)
                                                      from ticker_riskscore_onedimensional_weighted
                                                      where risk < 0
                                                  ), 0.) as risk_k_d
                      )

             select trod.symbol,
                    (1. + trod.risk / (case when trod.risk > 0 then s.risk_k_u else s.risk_k_d end)) /
                    2.               as risk_score --[0..1]
             from ticker_riskscore_onedimensional_weighted trod
                      left join scalekoefs as s on true -- one row
                      join {{ ref('tickers') }} using (symbol) -- ticker_metrics has somehow sometimes more tickers and sometimes less, so filter
         ),
     common_stocks as
         (
             select *
             from {{ ref('tickers') }}
             where "type" = 'common stock'
         ),

--loong list of today's known common stocks in the russel3000 index (grabbed from ishares etf)
--presave it as a table plz, instead of this ugly CTE
     stocks_list_russel3000 as
         (
             select symbol::varchar
             from (
                      select (json_each((etf_data -> 'Holdings')::json)).value ->> 'Code' as symbol
                      from {{ source('eod', 'eod_fundamentals') }}
                      where code = 'IWV'
                  ) t
                      join common_stocks using (symbol)
         ),


     -- prefiltered stocks + crypto list. (prefiltered common Stocks for mapping their risk onto Cryptos, both types in one list with "type" column)
     tickers_measuring_list as
         (
             --prefiltered stocks to measure the risk of crypto from
             with stocks_prefiltered as
                      (
                          with
                               stocks_specul_risk as
                                   (
                                       select tcc.symbol, tcc.sim_dif, "type"
                                       from {{ ref('ticker_categories_continuous') }} tcc
                                                join common_stocks using (symbol)
                                                join {{ ref('categories') }} c on c.id = tcc.category_id
                                       where c."name" = 'Speculation'
                                   ),
                               optimistic_sampling as
                                   (
                                       select r3l.symbol, ssr."type"
                                       from stocks_list_russel3000 r3l
                                                join stocks_specul_risk ssr using (symbol)
                                       where ssr.sim_dif > 0
                                   ),
                               to_add as (
                                             select greatest(3, 3 - (
                                                                        select count(*)
                                                                        from optimistic_sampling
                                                                    )) as val
                                         ), -- it's better to guarantee at least 2 or 3 stocks to use KNN further
                               pessimistic_upsampling as
                                   (
                                       select ssr.symbol, ssr."type"
                                       from stocks_specul_risk ssr
                                                left join optimistic_sampling os using (symbol)
                                       where os.symbol is null
                                       order by ssr.sim_dif desc
                                       limit (
                                                 select val
                                                 from to_add
                                             ) -- it's better to guarantee at least 2 or 3 stocks to use KNN further
                                   )
                               -- union optimistic sample and pessimistic upsample
                          select symbol, "type"
                          from optimistic_sampling
                          union
                          select symbol, "type"
                          from pessimistic_upsampling
                      )

                  -- prefiltered stocks (upsampled, if necessary (at least 2 or 3 for KNN))
             select symbol, "type"
             from stocks_prefiltered
             union
             --add cryptos
             select symbol, "type"
             from {{ ref('tickers') }}
             where "type" = 'crypto'
         ),

     tickers_return_volatility as -- just taking the volatility metric here and joining it with our list for measures
         (
             select tml.symbol,
                    tml."type",
                    ticker_momentum_metrics.stddev_3_years as std_dev
             from tickers_measuring_list tml
                      join {{ ref('ticker_momentum_metrics') }} using (symbol)
         ),

     crypto_nearest_stocks_by_return_volatility as
         (
             with dists as
                      ( -- it's cross join, but we have prefiltered the stocks to work with
                          select t1.symbol                                                               as symbol_cc,
                                 t2.symbol                                                               as symbol_st,
                                 abs(t1.std_dev - t2.std_dev)                                            as dist,
                                 row_number()
                                 over (partition by t1.symbol order by abs(t1.std_dev - t2.std_dev) asc) as rn
                          from tickers_return_volatility t1
                                   join tickers_return_volatility t2 on true
                          where t1."type" = 'crypto'
                            and t2."type" = 'common stock'
                      ),
                  proximities as
                      (
                          select *, -- +1e-30 in case if all neighbours are in the same coords or for one-nearest case
                                 ((1. - dist / (1e-30 + sum(dist) over (partition by symbol_cc))) + 1e-30) as proximity_weight
                          from dists d
                          where rn <= 2 -- edit for more neighbors here (while in 1 dimension - 2 points is ok (nearest left and nearest right))
                      )
             select *, -- we need this normalization in case of rn>2 or rn=1
                    proximity_weight / (sum(proximity_weight) over (partition by symbol_cc)) as proximity_nrmlzd_weight
             from proximities
         ),

     crypto_riskscore_bynearestneighbors as
         (
             select symbol_cc                                       as symbol,
                    sum(proximity_nrmlzd_weight * trscs.risk_score) as risk_score
             from crypto_nearest_stocks_by_return_volatility nn
                      join ticker_risk_score_common_stocks trscs on trscs.symbol = nn.symbol_st
             group by nn.symbol_cc
         )

select symbol, risk_score from crypto_riskscore_bynearestneighbors
union all
select symbol, risk_score from ticker_risk_score_common_stocks
