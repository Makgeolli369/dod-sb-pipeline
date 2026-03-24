with contracts as (
    select * from {{ ref('int_contract_net_obligations') }}
),

setaside_trends as (
    select
        action_date_fiscal_year,
        type_of_set_aside,
        type_of_set_aside_code,
        awarding_agency_name,

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
    group by
        action_date_fiscal_year,
        type_of_set_aside,
        type_of_set_aside_code,
        awarding_agency_name
)

select * from setaside_trends