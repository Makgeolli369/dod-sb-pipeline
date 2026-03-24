with fy2023 as (
    select * from {{ source('raw', 'contracts_fy2023') }}
),

fy2024 as (
    select * from {{ source('raw', 'contracts_fy2024') }}
),

fy2025 as (
    select * from {{ source('raw', 'contracts_fy2025') }}
),

fy2026 as (
    select * from {{ source('raw', 'contracts_fy2026') }}
),

unioned as (
    select * from fy2023
    union all
    select * from fy2024
    union all
    select * from fy2025
    union all
    select * from fy2026
)

select
    -- identifiers
    contract_transaction_unique_key,
    award_id_piid,
    modification_number,

    -- dates
    action_date,
    action_date_fiscal_year,

    -- agencies
    awarding_agency_name,
    awarding_sub_agency_name,

    -- vendor
    recipient_name,
    recipient_uei,

    -- financials
    federal_action_obligation,
    is_deobligation,

    -- contract type
    type_of_set_aside_code,
    type_of_set_aside,
    extent_competed,
    number_of_offers_received,

    -- classification
    naics_code,
    naics_description,
    product_or_service_code,

    -- geography
    primary_place_of_performance_state_code,

    -- small business certifications
    is_base_award,
    is_shutdown_period,

    -- data quality
    fy_data_quality_tier,
    fy_is_partial_year,
    fy_completeness_pct,
    fy_exclude_from_yoy,
    fy_status

from unioned
