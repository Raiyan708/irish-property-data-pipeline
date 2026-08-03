with staged as (
    select * from {{ ref('stg_property_price_summary') }}
),

with_prior_year as (
    select
        *,
        lag(median_price_eur) over (
            partition by county, property_type order by year
        ) as prior_year_median_price_eur
    from staged
)

select
    county,
    year,
    property_type,
    transaction_count,
    avg_price_eur,
    median_price_eur,
    min_price_eur,
    max_price_eur,
    median_price_eur - prior_year_median_price_eur as median_price_change_eur,
    round(
        100 * (median_price_eur - prior_year_median_price_eur) / prior_year_median_price_eur,
        2
    ) as median_price_change_pct
from with_prior_year
