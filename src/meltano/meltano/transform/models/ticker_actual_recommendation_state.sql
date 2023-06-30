select symbol,
       md5(coalesce(categories_state, '') || '_' || coalesce(interests_state, '') || '_' ||
           coalesce(round(risk_score::numeric, 4)::text, '')) as state_hash
from {{ ref('tickers') }}
         left join (
                       select symbol,
                              array_agg(category_id order by category_id)::text as categories_state
                       from {{ ref('ticker_categories') }}
                       group by symbol
                   ) categories_state using (symbol)
         left join (
                       select symbol,
                              array_agg(interest_id order by interest_id)::text as interests_state
                       from {{ ref('ticker_interests') }}
                       group by symbol
                   ) interests_state using (symbol)
         left join {{ ref('ticker_risk_scores') }} using (symbol)
where ms_enabled
  and (categories_state is not null
    or interests_state is not null
    or ticker_risk_scores.symbol is not null)