with contracts as (
    select * from {{ ref('stg_contracts') }}
),

vendor_summary as (
    select
        recipient_uei,
        recipient_name,

        -- obligation totals
        sum(federal_action_obligation) as total_net_obligation,
        sum(
            case
                when federal_action_obligation > 0
                    then federal_action_obligation
                else 0
            end
        ) as total_gross_obligation,

        -- transaction counts
        count(*) as total_transactions,
        count(distinct award_id_piid) as total_awards,
        count(distinct action_date_fiscal_year) as fiscal_years_active,
        count(distinct awarding_agency_name) as distinct_agencies,

        -- set aside diversity
        count(distinct type_of_set_aside_code) as distinct_set_aside_types,

        -- fiscal year range
        min(action_date_fiscal_year) as first_fiscal_year,
        max(action_date_fiscal_year) as last_fiscal_year,

        approx_top_count(type_of_set_aside, 1)[offset(0)].value
            as primary_set_aside,

        -- most common agency
        approx_top_count(awarding_agency_name, 1)[offset(0)].value
            as primary_agency,

        -- most common naics
        approx_top_count(naics_code, 1)[offset(0)].value
            as primary_naics_code,

        -- competition profile
        avg(number_of_offers_received) as avg_offers_received,

        -- data quality
        max(fy_data_quality_tier) as max_data_quality_tier

    from contracts
    where
        recipient_uei is not null
        and not is_shutdown_period
    group by
        recipient_uei,
        recipient_name
)

select * from vendor_summary
