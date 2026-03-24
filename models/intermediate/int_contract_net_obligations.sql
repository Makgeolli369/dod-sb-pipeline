with contracts as (
    select * from {{ ref('stg_contracts') }}
),

net_obligations as (
    select
        award_id_piid,
        recipient_name,
        recipient_uei,
        awarding_agency_name,
        awarding_sub_agency_name,
        naics_code,
        naics_description,
        product_or_service_code,
        type_of_set_aside_code,
        type_of_set_aside,
        extent_competed,
        primary_place_of_performance_state_code,
        fy_exclude_from_yoy,
        fy_is_partial_year,
        fy_status,
        action_date_fiscal_year,

        -- net obligation = sum of all transactions (base + modifications + de-oblig)
        sum(federal_action_obligation) as net_obligation,

        -- gross obligated (positive only)
        sum(
            case
                when federal_action_obligation > 0
                    then federal_action_obligation
                else 0
            end
        ) as gross_obligation,

        -- total de-obligated (negative only, stored as positive number)
        sum(
            case
                when federal_action_obligation < 0
                    then abs(federal_action_obligation)
                else 0
            end
        ) as total_deobligation,

        -- transaction counts
        count(*) as transaction_count,
        sum(
            case when is_deobligation then 1 else 0 end
        ) as deobligation_count,
        sum(
            case when is_base_award then 1 else 0 end
        ) as base_award_count,

        -- competition
        max(number_of_offers_received) as max_offers_received,

        -- dates
        min(action_date) as first_action_date,
        max(action_date) as last_action_date

    from contracts
    where not is_shutdown_period
    group by
        award_id_piid,
        recipient_name,
        recipient_uei,
        awarding_agency_name,
        awarding_sub_agency_name,
        naics_code,
        naics_description,
        product_or_service_code,
        type_of_set_aside_code,
        type_of_set_aside,
        extent_competed,
        primary_place_of_performance_state_code,
        fy_exclude_from_yoy,
        fy_is_partial_year,
        fy_status,
        action_date_fiscal_year
)

select * from net_obligations
