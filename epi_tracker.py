import streamlit as st
from utils.db import run_query
from queries.block_details import get_latest_event_date
from datetime import date, timedelta


@st.cache_data(ttl=3600)
def fetch_epi_tracker(partners=None, block_ids=None, block_names=None):

    # --------------------------------------------------
    # 1. STATIC DATE LOGIC (V1)
    # --------------------------------------------------
    end_date = get_latest_event_date()
    
    start_date = end_date - timedelta(days=44)

    calc_start_date = end_date - timedelta(days=89)

    partners_selected = bool(partners)

    # --------------------------------------------------
    # 2. WHERE CLAUSE (BASE DATA ONLY)
    # --------------------------------------------------
    conditions = [
        f"eventDate BETWEEN '{calc_start_date}' AND '{end_date}'",
        "est_earnings > 5 and uniq_impr > 50",
        "partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')",
    ]

    if partners:
        partner_list = "', '".join(partners)
        conditions.append(f"partner IN ('{partner_list}')")

    if block_ids:
        block_id_list = ",".join(map(str, block_ids))
        conditions.append(f"keyword_block_id IN ({block_id_list})")

    if block_names:
        block_name_list = "', '".join(block_names)
        conditions.append(f"block_name IN ('{block_name_list}')")

    where_clause = " AND ".join(conditions)

    # Optional: reuse partner filter inside partner share CTE for speed (only when partners selected)
    # partner_filter_sql = ""
    # if partners:
    #     partner_list = "', '".join(partners)
    #     partner_filter_sql = f"AND partner IN ('{partner_list}')"

    # --------------------------------------------------
    # 3. CONDITIONAL PARTNER REVENUE SHARE
    # --------------------------------------------------
    # partner_share_cte = ""
    # partner_share_join = ""
    # partner_share_select = ""

    # if partners_selected:
    #     partner_share_cte = f"""
    #     , partner_block_revenue_share AS (
    #         SELECT
    #             partner,
    #             keyword_block_id,
    #             SUM(est_earnings) AS partner_45d_earnings,
    #             SUM(est_earnings) * 1.0
    #               / NULLIF(SUM(SUM(est_earnings)) OVER (PARTITION BY partner), 0) AS partner_revenue_share
    #         FROM team_block_stats
    #         WHERE eventDate between '{start_date}' and '{end_date}'
    #           AND est_earnings > 0
    #           AND partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
    #           {partner_filter_sql}
    #         GROUP BY partner, keyword_block_id
    #     )
    #     """

    #     partner_share_join = """
    #     LEFT JOIN partner_block_revenue_share p
    #       ON s.partner = p.partner
    #      AND s.keyword_block_id = p.keyword_block_id
    #     """

    #     partner_share_select = """
    #     , ROUND(p.partner_revenue_share * 100, 2) AS `Partner Share`
    #     """

    # --------------------------------------------------
    # 4. FINAL QUERY
    # --------------------------------------------------
    query = f"""
    WITH base AS (
        SELECT
            eventDate,
            partner,
            keyword_block_id,
            block_name,
            est_earnings,
            uniq_impr,
            epi,
            paid_clicks
        FROM team_block_stats
        WHERE {where_clause}
    ),

    rolling AS (
        SELECT
            b.*,
            SUM(est_earnings) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(uniq_impr) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS epi_7d_avg,

            SUM(est_earnings) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(uniq_impr) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS epi_30d_avg,
            
            AVG(b.est_earnings) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS earn_7d_avg,

            AVG(b.est_earnings) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS earn_30d_avg,
            
            LAG(b.epi) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate) AS prev_epi,
            
            avg(b.uniq_impr) over (partition by b.keyword_block_id order by b.eventDate rows between 6 preceding and current row) as impr_7d_avg
        FROM base b
    ),
    
    flagged AS (
        SELECT
            r.*,

            -- Low EPI flags
            CASE WHEN r.epi < r.epi_7d_avg * 0.8 THEN 1 ELSE 0 END AS low_epi_flag,
            CASE WHEN r.epi < r.epi_7d_avg * 0.8 AND r.est_earnings < r.earn_7d_avg * 0.95 THEN 1 ELSE 0 END AS low_epi_low_rev_flag,
            CASE WHEN r.epi < r.epi_7d_avg * 0.8 AND r.est_earnings > r.earn_7d_avg * 0.90 THEN 1 ELSE 0 END AS low_epi_high_rev_flag,

            -- High EPI flags
            CASE WHEN r.epi > r.epi_7d_avg * 1.15 THEN 1 ELSE 0 END AS high_epi_flag,
            CASE WHEN r.epi > r.epi_7d_avg * 1.15 AND r.est_earnings > r.earn_7d_avg * 0.90 THEN 1 ELSE 0 END AS high_epi_high_rev_flag,
            CASE WHEN r.epi > r.epi_7d_avg * 1.15 AND r.est_earnings < r.earn_7d_avg * 0.95 THEN 1 ELSE 0 END AS high_epi_low_rev_flag,

            -- Sharp moves vs previous day
            CASE WHEN r.prev_epi IS NOT NULL AND r.epi < r.prev_epi * 0.5 THEN 1 ELSE 0 END AS sharp_drop_flag,
            CASE WHEN r.prev_epi IS NOT NULL AND r.epi > r.prev_epi * 1.5 THEN 1 ELSE 0 END AS sharp_rise_flag,

            -- Daily Declines/Rises
            CASE WHEN r.epi < r.prev_epi THEN 1 ELSE 0 END AS mild_epi_drift_flag,
            case when r.epi > r.prev_epi then 1 else 0 end as mild_epi_rise_flag,

            -- Group ids to measure consecutive runs (streaks)
            case when r.epi < r.prev_epi then
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
            - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epi < r.prev_epi) ORDER BY r.eventDate) else null end AS grp_daily_drop,

            case when r.epi > r.prev_epi then
            row_number() over (partition by r.keyword_block_id order by r.eventDate)
            - row_number() over (partition by r.keyword_block_id, (r.epi > r.prev_epi) order by r.eventDate) else null end as grp_daily_rise,

            case when r.epi < r.epi_7d_avg * 0.8 then
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epi < r.epi_7d_avg * 0.8) ORDER BY r.eventDate)
            else null end AS grp_low,

            case when r.epi > r.epi_7d_avg * 1.15 then
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epi > r.epi_7d_avg * 1.15) ORDER BY r.eventDate)
            else null end AS grp_high,

            -- Volume-led EPI stagnation
            CASE 
                WHEN r.uniq_impr > r.impr_7d_avg * 1.8 AND r.uniq_impr > 900
                  AND r.epi BETWEEN r.epi_7d_avg * 0.85 AND r.epi_7d_avg * 1.05 AND r.est_earnings > r.earn_7d_avg * 1.3
                  AND ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate) 
            		- ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.uniq_impr > r.impr_7d_avg * 1.8 AND r.epi BETWEEN r.epi_7d_avg * 0.85 AND r.epi_7d_avg * 1.05) 
            			ORDER BY r.eventDate) = 1
                THEN 1 
                ELSE 0
            END AS volume_epi_stagnation_flag

        FROM rolling r
    ),

    streaked AS (
        SELECT
            f.*,
            CASE WHEN f.low_epi_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_low) ELSE 0 END AS low_streak_len,
            CASE WHEN f.high_epi_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_high) ELSE 0 END AS high_streak_len,
            CASE WHEN f.mild_epi_drift_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_daily_drop) ELSE 0 END AS mild_drift_streak_len,
            CASE WHEN f.mild_epi_rise_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_daily_rise) ELSE 0 END AS mild_rise_streak_len,

            CASE 
                WHEN f.mild_epi_drift_flag = 1 
                    THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_daily_drop ORDER BY f.eventDate DESC) = 1 
                        ELSE 0 
                END AS is_current_drop_streak_end,
        
            CASE 
                WHEN f.mild_epi_rise_flag = 1 
                    THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_daily_rise ORDER BY f.eventDate DESC) = 1 
                        ELSE 0 
            END AS is_current_rise_streak_end,
    
            CASE 
                    WHEN f.low_epi_flag = 1 
                        THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_low ORDER BY f.eventDate DESC) = 1 
                        ELSE 0 
                END AS is_current_low_streak_end,            
            
            CASE 
                WHEN f.high_epi_flag = 1 
                    THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_high ORDER BY f.eventDate DESC) = 1 
                    ELSE 0 
            END AS is_current_high_streak_end,

        ROUND(((f.epi - f.epi_7d_avg) / NULLIF(f.epi_7d_avg, 0)) * 100, 1) AS epi_perf_pct,
        
        ROUND(((f.est_earnings - f.earn_7d_avg) / NULLIF(f.earn_7d_avg, 0)) * 100, 1) AS rev_perf_pct

            
        FROM flagged f
    ),

    global_block_revenue_share AS (
        SELECT
            t.eventDate, t.keyword_block_id,
            (t.est_earnings * 1.0) / nullif(sum(t.est_earnings) over (partition by t.eventDate), 0) as daily_rev_share
        FROM team_block_stats t
        WHERE t.eventDate between '{calc_start_date}' and '{end_date}'
          AND t.est_earnings > 5 and t.uniq_impr > 50
          AND t.partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
        GROUP BY t.keyword_block_id, t.eventDate
    ),
    
    
    final AS (
        SELECT
            s.eventDate AS Date,
            s.partner AS Partner,
            s.keyword_block_id AS `Block ID`,
            s.block_name AS `Block Name`,
            ROUND(s.est_earnings, 2) AS Earnings,
            ROUND(s.earn_7d_avg, 2) AS `7D Avg Earnings`,
            round(s.earn_30d_avg, 2) as `30D Avg Earnings`,
            s.uniq_impr AS Impressions,
            s.paid_clicks AS Clicks,
            ROUND(s.epi, 4) AS EPI,
            ROUND(s.epi_7d_avg, 4) AS `7D Avg EPI`,
            round(s.epi_30d_avg, 4) as `30D Avg EPI`,
            
            round(g.daily_rev_share * 100, 2) as `Block's Daily Share`,

            round(impr_7d_avg, 2) as `7D Avg Impressions`,

            CASE
                -- 1) CRITICAL RED (streak / sharp drops)
                WHEN s.low_streak_len >= 3 AND g.daily_rev_share > 0.015 and s.is_current_low_streak_end = 1 THEN CONCAT('🔥 EPI ↓ ', s.low_streak_len, 'D Streak | ', s.epi_perf_pct, '% vs 7D Avg | High Rev Block')
                
                WHEN s.low_streak_len >= 3 and s.is_current_low_streak_end = 1 then CONCAT('⚠️ EPI ↓ ', s.low_streak_len, 'D Streak | ', s.epi_perf_pct, '% vs 7D Avg')
                
                WHEN s.sharp_drop_flag = 1 AND g.daily_rev_share > 0.015 THEN CONCAT('🚨 Sharp EPI ↓ ', ROUND(((s.epi - s.prev_epi) / NULLIF(s.prev_epi, 0)) * 100, 2), '% vs Yesterday | High Rev Block')
                
                WHEN s.sharp_drop_flag = 1 THEN
                CONCAT('🚨 Sharp EPI ↓ ', ROUND(((s.epi - s.prev_epi) / NULLIF(s.prev_epi, 0)) * 100, 2), '% vs Yesterday')

                
                -- 2) CRITICAL GREEN (streak / sharp rises)
                WHEN s.high_streak_len >= 3 AND g.daily_rev_share > 0.015 and s.is_current_low_streak_end = 1 THEN CONCAT('🔥 EPI ↑ ', s.high_streak_len, 'D Streak | ', s.epi_perf_pct, '% vs 7D Avg | High Rev Block')
                WHEN s.high_streak_len >= 3 and s.is_current_low_streak_end = 1 THEN CONCAT('📈 EPI ↑ ', s.low_streak_len, 'D Streak | ', s.epi_perf_pct, '% vs 7D Avg')
                WHEN s.sharp_rise_flag = 1 AND g.daily_rev_share > 0.015 THEN CONCAT('🏆 Sharp EPI ↑ ', ROUND(((s.epi - s.prev_epi) / NULLIF(s.prev_epi, 0)) * 100, 2), '% vs Yesterday | High Rev Block')
                WHEN s.sharp_rise_flag = 1 THEN
                CONCAT('✨ Sharp EPI ↑ ', ROUND(((s.epi - s.prev_epi) / NULLIF(s.prev_epi, 0)) * 100, 2), '% vs Yesterday')

                -- 3) DAILY MOVERS

                WHEN s.mild_drift_streak_len >= 4 AND s.is_current_drop_streak_end = 1 
                    THEN concat('👀 EPI Declining Daily For ', s.mild_drift_streak_len, ' Days')
                
                WHEN s.mild_rise_streak_len >= 4 AND s.is_current_rise_streak_end = 1 
                    THEN concat('🏆 EPI Rising Daily For ', s.mild_rise_streak_len, ' Days')

                -- 4) SINGLE-DAY diagnostics
                WHEN s.low_epi_low_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN '❌ Both EPI & Rev Low | High Rev Block'
                WHEN s.low_epi_high_rev_flag = 1 THEN CONCAT('⚠️ EPI: ', s.epi_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')

                WHEN s.high_epi_high_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN '✅ EPI & Revenue Both High | High Rev Block'
                WHEN s.high_epi_low_rev_flag = 1 then CONCAT('✨ EPI: ', s.epi_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')
                WHEN s.high_epi_high_rev_flag = 1 THEN '✅ EPI & Revenue Both Higher Than 7D Avg'

                WHEN s.low_epi_flag = 1 AND g.daily_rev_share > 0.015 THEN '🔎 Low EPI - High Revenue Block'
                WHEN s.high_epi_flag = 1 AND g.daily_rev_share > 0.015 THEN '🔎 High EPI - High Revenue Block'
                
                WHEN s.low_epi_flag = 1 THEN CONCAT('⚠️ EPI DOWN | ', s.epi_perf_pct, '% vs 7D Avg')
                WHEN s.high_epi_flag = 1 THEN CONCAT('📈 EPI UP | ', s.epi_perf_pct, '% vs 7D Avg')

                -- 🟡 SCALE QUALITY WATCH
            	WHEN s.volume_epi_stagnation_flag = 1 AND g.daily_rev_share > 0.01 THEN '📈 Traffic & Revenue Spike with Stable EPI on High Revenue Block'
                WHEN s.volume_epi_stagnation_flag = 1 THEN '📈 Traffic & Revenue Spike with Stable EPI'
                

                ELSE 'Within Thresholds'
            END AS Alerts,

            CASE WHEN g.daily_rev_share > 0.015 THEN 1 ELSE 0 END AS is_high_revenue_block,

            s.high_streak_len, s.sharp_rise_flag, s.high_epi_high_rev_flag, s.high_epi_low_rev_flag, s.high_epi_flag, s.mild_rise_streak_len,
            s.low_streak_len, s.sharp_drop_flag, s.low_epi_low_rev_flag, s.low_epi_high_rev_flag, s.low_epi_flag, s.mild_drift_streak_len,
            s.volume_epi_stagnation_flag

        FROM streaked s
        LEFT JOIN global_block_revenue_share g ON s.keyword_block_id = g.keyword_block_id AND s.eventDate = g.eventDate
        
    )

    SELECT 
        Date, Alerts, `Block ID`,
    	Partner,
    	`Block Name`,
    	Earnings,
    	`7D Avg Earnings`, `30D Avg Earnings`,
    	Impressions,
    	Clicks,
    	EPI, 
        `7D Avg EPI`, `30D Avg EPI`,
        `Block's Daily Share`,
        

        `7D Avg Impressions`,

        is_high_revenue_block,
        
        CASE
                when `Alerts` = 'Within Thresholds' then 'no impact'
                
                -- 🟢 GREEN : Positive outliers
                when high_streak_len >= 3
                  or sharp_rise_flag = 1
                  or high_epi_high_rev_flag = 1
                  or high_epi_low_rev_flag = 1
                  or high_epi_flag = 1
                  or mild_rise_streak_len >= 4
                then 'green'
    
                -- 🔥 RED : Material problems
                when low_streak_len >= 3
                  or sharp_drop_flag = 1
                  or low_epi_low_rev_flag = 1
                  or low_epi_high_rev_flag = 1
                  or low_epi_flag = 1
                  or mild_drift_streak_len >= 4
                  or volume_epi_stagnation_flag = 1
                then 'red'                
    
                ELSE 'no impact'
            
            END AS alert_bucket
    
    FROM final
    WHERE Date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY Date DESC, Earnings DESC;
    """

    df = run_query(query)
    return df
