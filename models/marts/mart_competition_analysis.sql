with contracts as (
    select * from {{ ref('int_contract_net_obligations') }}
),

competition_analysis as (
    select
        action_date_fiscal_year,
        awarding_agency_name,
        awarding_sub_agency_name,
        extent_competed,
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

        -- competition metrics
        avg(max_offers_received) as avg_offers_received,
        sum(base_award_count) as total_base_awards,
        sum(deobligation_count) as total_deobligations,

        -- single vs multiple offer breakdown
        sum(case when max_offers_received = 1 
            then 1 else 0 end) as single_offer_awards,
        sum(case when max_offers_received > 1 
            then 1 else 0 end) as multiple_offer_awards,

        -- data quality
        max(case when fy_exclude_from_yoy then 1 else 0 end) as has_excluded_fy,
        max(case when fy_is_partial_year then 1 else 0 end) as has_partial_year

    from contracts
    group by
        action_date_fiscal_year,
        awarding_agency_name,
        awarding_sub_agency_name,
        extent_competed,
        type_of_set_aside,
        type_of_set_aside_code,
        naics_code,
        naics_description
)

select * from competition_analysis
