with contracts as (
    select * from {{ ref('int_contract_net_obligations') }}
),

geo_spend as (
    select
        primary_place_of_performance_state_code,
        action_date_fiscal_year,
        awarding_agency_name,
        awarding_sub_agency_name,
        type_of_set_aside,
        type_of_set_aside_code,
        naics_code,
        naics_description,

        -- obligations
        sum(net_obligation) as total_net_obligation,
        sum(gross_obligation) as total_gross_obligation,
        sum(total_deobligation) as total_deobligation,

        -- counts
        count(distinct award_id_piid) as total_awards,
        count(distinct recipient_uei) as total_vendors,
        sum(transaction_count) as total_transactions,
        sum(deobligation_count) as total_deobligations,

        -- competition
        avg(max_offers_received) as avg_offers_received,

        -- data quality
        max(case when fy_exclude_from_yoy then 1 else 0 end) as has_excluded_fy,
        max(case when fy_is_partial_year then 1 else 0 end) as has_partial_year

    from contracts
    where primary_place_of_performance_state_code is not null
    group by
        primary_place_of_performance_state_code,
        action_date_fiscal_year,
        awarding_agency_name,
        awarding_sub_agency_name,
        type_of_set_aside,
        type_of_set_aside_code,
        naics_code,
        naics_description
)

select * from geo_spend
