with net_obligations as (
    select * from {{ ref('int_contract_net_obligations') }}
),

vendor_certs as (
    select * from {{ ref('int_vendor_certifications') }}
),

vendor_performance as (
    select
        n.recipient_uei,
        n.recipient_name,
        n.action_date_fiscal_year,
        n.awarding_agency_name,
        n.awarding_sub_agency_name,
        n.type_of_set_aside,
        n.type_of_set_aside_code,
        n.naics_code,
        n.naics_description,
        n.primary_place_of_performance_state_code,

        -- obligations
        sum(n.net_obligation) as total_net_obligation,
        sum(n.gross_obligation) as total_gross_obligation,
        sum(n.total_deobligation) as total_deobligation,

        -- counts
        count(distinct n.award_id_piid) as total_awards,
        sum(n.transaction_count) as total_transactions,
        sum(n.deobligation_count) as total_deobligations,

        -- competition
        avg(n.max_offers_received) as avg_offers_received,

        -- vendor profile from certifications model
        v.total_awards as vendor_lifetime_awards,
        v.total_net_obligation as vendor_lifetime_obligation,
        v.fiscal_years_active,
        v.distinct_agencies,
        v.primary_set_aside,
        v.primary_agency,
        v.primary_naics_code,

        -- data quality
        max(case when n.fy_exclude_from_yoy then 1 else 0 end) as has_excluded_fy,
        max(case when n.fy_is_partial_year then 1 else 0 end) as has_partial_year

    from net_obligations n
    left join vendor_certs v
        on n.recipient_uei = v.recipient_uei
    group by
        n.recipient_uei,
        n.recipient_name,
        n.action_date_fiscal_year,
        n.awarding_agency_name,
        n.awarding_sub_agency_name,
        n.type_of_set_aside,
        n.type_of_set_aside_code,
        n.naics_code,
        n.naics_description,
        n.primary_place_of_performance_state_code,
        v.total_awards,
        v.total_net_obligation,
        v.fiscal_years_active,
        v.distinct_agencies,
        v.primary_set_aside,
        v.primary_agency,
        v.primary_naics_code
)

select * from vendor_performance
