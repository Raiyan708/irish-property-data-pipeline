select
    county,
    year,
    description_of_property as property_type,
    transaction_count,
    avg_price_eur,
    median_price_eur,
    min_price_eur,
    max_price_eur
from {{ source('warehouse', 'county_yearly_price_summary') }}
