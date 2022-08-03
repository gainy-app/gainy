{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}


with 
common_stocks as
         (
             select *
             from {{ ref('tickers') }}
             where "type" = 'common stock'
         ),
         
ticker_risk_score_common_stocks as
         (	
            with
            return_volatility as 
              (
                select 
                  cs.symbol,
                  ticker_metrics.stddev_3_years	as retvolat
                from common_stocks cs
                  join {{ ref('ticker_metrics') }} using (symbol)
              ),
            volat_cntr as 
              (
                select percentile_cont(0.5) within group (order by retvolat asc) as val 
                from return_volatility
              ),
            volat_centered as --half of tickers on the left and half on the right
              (
                select symbol, retvolat - cntr.val as retvolat
                from return_volatility
                  left join volat_cntr cntr on true
              ),
            volat_scale_coefs as 
              (
                select 
                  1e-30 + coalesce((select percentile_cont(0.5) within group(order by retvolat asc) from volat_centered where retvolat > 0), 0.) as retvolat_u,
                  1e-30 + coalesce((select percentile_cont(0.5) within group(order by -retvolat asc) from volat_centered where retvolat < 0), 0.) as retvolat_d
              ),
            volat_cdf_c as
              (
                select
                  symbol, 
                  2.*pnorm((retvolat/(case when retvolat>0 then s.retvolat_u else s.retvolat_d end)) *0.67449) -1. as retvolat
                  -- *0.67449 sigma rescaling so that the median(equalized to 1sigma) appears precicely on the quartiles 0.25 and 0.75 of the CDF output
                from
                  volat_centered
                  left join volat_scale_coefs s on true
              ),
            volat_cdf_c_scale_coefs as
              (
                select 
                  1e-30 + coalesce((select MAX(retvolat) from volat_cdf_c where retvolat > 0), 0.) as cdf_c_u,
                  1e-30 + coalesce((select MAX(-retvolat) from volat_cdf_c where retvolat < 0), 0.) as cdf_c_d
              ),
            ticker_risk as
              (
                select
                  symbol,
                  (1.+retvolat/(case when retvolat>0 then s.cdf_c_u else s.cdf_c_d end))/2. as risk_score
                from
                  volat_cdf_c
                  left join volat_cdf_c_scale_coefs s on true
              )
            select * from ticker_risk
         ),
     complex_ticker_categories as
         (
             select ticker_components.symbol,
                    sum(ticker_risk_score_common_stocks.risk_score * component_weight) / sum(component_weight) as risk_score
             from {{ ref('ticker_components') }}
                      join ticker_risk_score_common_stocks
                           on ticker_risk_score_common_stocks.symbol = ticker_components.component_symbol
             group by ticker_components.symbol
             having sum(component_weight) > 0
         ),
         


--CRYPTO--

--loong list of today's known common stocks in the russel3000 index (grabbed from ishares etf)
--presave it as a table plz, instead of this ugly CTE
     stocks_list_russel3000 as
         (
             select ticker_components.component_symbol as symbol
             from {{ ref('ticker_components')}}
                 join common_stocks on common_stocks.symbol = ticker_components.component_symbol
             where ticker_components.symbol = 'IWV'
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
                    ticker_metrics.stddev_3_years as std_dev
             from tickers_measuring_list tml
                      join {{ ref('ticker_metrics') }} using (symbol)
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
union all
select symbol, risk_score from complex_ticker_categories
