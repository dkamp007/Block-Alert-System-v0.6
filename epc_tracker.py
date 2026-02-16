import streamlit as st
from utils.db import run_query
from queries.block_details import get_latest_event_date
from datetime import timedelta


@st.cache_data(ttl=3600)
def fetch_epc_tracker(partners=None, block_ids=None, block_names=None):

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

    # --------------------------------------------------
    # 4. OPTIMIZED QUERY
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
            paid_clicks,
            epc
        FROM team_block_stats
        WHERE {where_clause}
    ),

    rolling AS (
        SELECT
            b.*,
            SUM(est_earnings) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(paid_clicks) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS epc_7d_avg,

            SUM(est_earnings) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(paid_clicks) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS epc_30d_avg,
            
            AVG(b.est_earnings) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS earn_7d_avg,
            
            AVG(b.est_earnings) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS earn_30d_avg,
            
            LAG(b.epc) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate) AS prev_epc,
            
            AVG(b.uniq_impr) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS impr_7d_avg
        
        FROM base b
    ),
    

    flagged AS (
        SELECT
            r.*,

            -- Low EPC flags
            CASE WHEN r.epc < r.epc_7d_avg * 0.8 THEN 1 ELSE 0 END AS low_epc_flag,
            CASE WHEN r.epc < r.epc_7d_avg * 0.8 AND r.est_earnings < r.earn_7d_avg * 0.95 THEN 1 ELSE 0 END AS low_epc_low_rev_flag,
            CASE WHEN r.epc < r.epc_7d_avg * 0.8 AND r.est_earnings > r.earn_7d_avg * 0.90 THEN 1 ELSE 0 END AS low_epc_high_rev_flag,

            -- High EPC flags
            CASE WHEN r.epc > r.epc_7d_avg * 1.15 THEN 1 ELSE 0 END AS high_epc_flag,
            CASE WHEN r.epc > r.epc_7d_avg * 1.15 AND r.est_earnings > r.earn_7d_avg * 0.90 THEN 1 ELSE 0 END AS high_epc_high_rev_flag,
            CASE WHEN r.epc > r.epc_7d_avg * 1.15 AND r.est_earnings < r.earn_7d_avg * 0.95 THEN 1 ELSE 0 END AS high_epc_low_rev_flag,

            -- Sharp moves vs previous day
            CASE WHEN r.prev_epc IS NOT NULL AND r.epc < r.prev_epc * 0.5 THEN 1 ELSE 0 END AS sharp_drop_flag,
            CASE WHEN r.prev_epc IS NOT NULL AND r.epc > r.prev_epc * 1.5 THEN 1 ELSE 0 END AS sharp_rise_flag,

            -- Daily Declines/Rises
            CASE WHEN r.epc < r.prev_epc THEN 1 ELSE 0 END AS mild_epc_drift_flag,
            case when r.epc > r.prev_epc then 1 else 0 end as mild_epc_rise_flag,

            -- Group ids to measure consecutive runs (streaks)
            CASE WHEN r.epc < r.prev_epc THEN             
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
            - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epc < r.prev_epc) ORDER BY r.eventDate)
            else null end AS grp_daily_drop,

            CASE WHEN r.epc > r.prev_epc THEN 
            row_number() over (partition by r.keyword_block_id order by r.eventDate)
            - row_number() over (partition by r.keyword_block_id, (r.epc > r.prev_epc) order by r.eventDate)
            else null end as grp_daily_rise,

            case when r.epc < r.epc_7d_avg * 0.8 then
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epc < r.epc_7d_avg * 0.8) ORDER BY r.eventDate)
            else null end AS grp_low,

            case when r.epc > r.epc_7d_avg * 1.15 then
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epc > r.epc_7d_avg * 1.15) ORDER BY r.eventDate) 
              else null end AS grp_high,

            -- Volume-led EPC stagnation
            CASE 
                WHEN r.uniq_impr > r.impr_7d_avg * 1.8 AND r.uniq_impr > 900
                  AND r.epc BETWEEN r.epc_7d_avg * 0.85 AND r.epc_7d_avg * 1.05 AND r.est_earnings > r.earn_7d_avg * 1.3
                  AND ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate) 
            		- ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.uniq_impr > r.impr_7d_avg * 1.8 AND r.epc BETWEEN r.epc_7d_avg * 0.85 AND r.epc_7d_avg * 1.05) 
            			ORDER BY r.eventDate) = 1
                THEN 1 
                ELSE 0
            END AS volume_epc_stagnation_flag

        FROM rolling r
    ),

    streaked AS (
        SELECT
            f.*,
            CASE WHEN f.low_epc_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_low) ELSE 0 END AS low_streak_len,
            CASE WHEN f.high_epc_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_high) ELSE 0 END AS high_streak_len,
            CASE WHEN f.mild_epc_drift_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_daily_drop) ELSE 0 END AS mild_drift_streak_len,
            CASE WHEN f.mild_epc_rise_flag = 1 THEN COUNT(*) OVER (PARTITION BY f.keyword_block_id, f.grp_daily_rise) ELSE 0 END AS mild_rise_streak_len,

            CASE 
                WHEN f.mild_epc_drift_flag = 1 
                    THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_daily_drop ORDER BY f.eventDate DESC) = 1 
                        ELSE 0 
            END AS is_current_drop_streak_end,
        
            CASE 
                WHEN f.mild_epc_rise_flag = 1 
                    THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_daily_rise ORDER BY f.eventDate DESC) = 1 
                        ELSE 0 
            END AS is_current_rise_streak_end,

            CASE 
                WHEN f.low_epc_flag = 1 
                    THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_low ORDER BY f.eventDate DESC) = 1 
                    ELSE 0 
            END AS is_current_low_streak_end,            
            
            CASE 
                WHEN f.high_epc_flag = 1 
                    THEN ROW_NUMBER() OVER (PARTITION BY f.keyword_block_id, f.grp_high ORDER BY f.eventDate DESC) = 1 
                    ELSE 0 
            END AS is_current_high_streak_end,
        
        

        ROUND(((f.epc - f.epc_7d_avg) / NULLIF(f.epc_7d_avg, 0)) * 100, 2) AS epc_perf_pct,
        
        ROUND(((f.est_earnings - f.earn_7d_avg) / NULLIF(f.earn_7d_avg, 0)) * 100, 2) AS rev_perf_pct
            
        FROM flagged f
    ),

    
    global_block_revenue_share AS (
        SELECT
            t.eventDate, t.keyword_block_id,
            --SUM(t.est_earnings) * 1.0 / NULLIF(SUM(SUM(t.est_earnings)) OVER (), 0) AS global_revenue_share,
            (t.est_earnings * 1.0) / nullif(sum(sum(t.est_earnings)) over (partition by t.eventDate), 0) as daily_rev_share
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
            ROUND(s.epc, 4) AS EPC,
            ROUND(s.epc_7d_avg, 4) AS `7D Avg EPC`,
            round(s.epc_30d_avg, 4) as `30D Avg EPC`,
            
            round(g.daily_rev_share * 100, 2) as `Block's Daily Share`,

            round(impr_7d_avg, 2) as `7D Avg Impressions`,
            


            CASE
                -- 1) CRITICAL RED (streak / sharp drops)
                WHEN s.low_streak_len >= 3 AND g.daily_rev_share > 0.015 AND s.is_current_low_streak_end = 1 THEN CONCAT('🔥 EPC ↓ ', s.low_streak_len, 'D Streak | ', s.epc_perf_pct, '% vs 7D Avg | High Rev Block')                
                
                WHEN s.low_streak_len >= 3 AND s.is_current_low_streak_end = 1 THEN CONCAT('⚠️ EPC ↓ ', s.low_streak_len, 'D Streak | ', s.epc_perf_pct, '% vs 7D Avg')               
                WHEN s.sharp_drop_flag = 1 AND g.daily_rev_share > 0.015 THEN CONCAT('🚨 Sharp EPC ↓ ', ROUND(((s.epc - s.prev_epc) / NULLIF(s.prev_epc, 0)) * 100, 2), '% vs Yesterday | High Rev Block')
                
                WHEN s.sharp_drop_flag = 1 THEN CONCAT('🚨 Sharp EPC ↓ ', ROUND(((s.epc - s.prev_epc) / NULLIF(s.prev_epc, 0)) * 100, 2), '% vs Yesterday')

                
                -- 2) CRITICAL GREEN (streak / sharp rises)
                WHEN s.high_streak_len >= 3 AND g.daily_rev_share > 0.015 AND s.is_current_high_streak_end = 1 THEN CONCAT('🔥 EPC ↑ ', s.high_streak_len, 'D Streak | ', s.epc_perf_pct, '% vs 7D Avg | High Rev Block')
                
                WHEN s.high_streak_len >= 3 AND s.is_current_high_streak_end = 1 THEN CONCAT('📈 EPC ↑ ', s.low_streak_len, 'D Streak | ', s.epc_perf_pct, '% vs 7D Avg')                
                WHEN s.sharp_rise_flag = 1 AND g.daily_rev_share > 0.015 THEN CONCAT('🏆 Sharp EPC ↑ ', ROUND(((s.epc - s.prev_epc) / NULLIF(s.prev_epc, 0)) * 100, 2), '% vs Yesterday | High Rev Block')
                
                WHEN s.sharp_rise_flag = 1 then CONCAT('✨ Sharp EPC ↑ ', ROUND(((s.epc - s.prev_epc) / NULLIF(s.prev_epc, 0)) * 100, 2), '% vs Yesterday')

                
                -- 3) DAILY MOVERS

                WHEN s.mild_drift_streak_len >= 4 AND s.is_current_drop_streak_end = 1 
                    THEN concat('👀 EPC Declining Daily For ', s.mild_drift_streak_len, ' Days')
                
                WHEN s.mild_rise_streak_len >= 4 AND s.is_current_rise_streak_end = 1 
                    THEN concat('🏆 EPC Rising Daily For ', s.mild_rise_streak_len, ' Days')

                
                -- 4) SINGLE-DAY diagnostics
                WHEN s.low_epc_low_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN '❌ Both EPC & Rev Low | High Rev Block'
                WHEN s.low_epc_high_rev_flag = 1 THEN CONCAT('⚠️ EPC: ', s.epc_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')
                

                WHEN s.high_epc_high_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN '✅ EPC & Revenue Both High | High Rev Block'
                WHEN s.high_epc_low_rev_flag = 1 THEN CONCAT('✨ EPC: ', s.epc_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')
                WHEN s.high_epc_high_rev_flag = 1 THEN '✅ EPC & Revenue Both Higher Than 7D Avg'

                
                WHEN s.low_epc_flag = 1 AND g.daily_rev_share > 0.015 THEN '🔎 Low EPC - High Revenue Block'
                WHEN s.high_epc_flag = 1 AND g.daily_rev_share > 0.015 THEN '🔎 High EPC - High Revenue Block'
                

                WHEN s.low_epc_flag = 1 THEN CONCAT('⚠️ EPC DOWN | ', s.epc_perf_pct, '% vs 7D Avg')
                WHEN s.high_epc_flag = 1 THEN CONCAT('📈 EPC UP | ', s.epc_perf_pct, '% vs 7D Avg')

                
                -- 5) SCALE QUALITY WATCH
            	WHEN s.volume_epc_stagnation_flag = 1 AND g.daily_rev_share > 0.01 THEN '📈 Traffic & Revenue Spike with Stable EPC on High Revenue Block'
                WHEN s.volume_epc_stagnation_flag = 1 THEN '📈 Traffic & Revenue Spike with Stable EPC'

                ELSE 'Within Thresholds'
                
            END AS Alerts,

            CASE WHEN g.daily_rev_share > 0.015 THEN 1 ELSE 0 END AS is_high_revenue_block,

            s.high_streak_len, s.sharp_rise_flag, s.high_epc_high_rev_flag, s.high_epc_low_rev_flag, s.high_epc_flag, s.mild_rise_streak_len,
            s.low_streak_len, s.sharp_drop_flag, s.low_epc_low_rev_flag, s.low_epc_high_rev_flag, s.low_epc_flag, s.mild_drift_streak_len,
            s.volume_epc_stagnation_flag
            
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
    	EPC, 
        `7D Avg EPC`, `30D Avg EPC`, `Block's Daily Share`,

        `7D Avg Impressions`,

        is_high_revenue_block,
        
        


        CASE
                when `Alerts` = 'Within Thresholds' then 'no impact'
                
                -- 🟢 GREEN : Positive outliers
                when high_streak_len >= 3
                  or sharp_rise_flag = 1
                  or high_epc_high_rev_flag = 1
                  or high_epc_low_rev_flag = 1
                  or high_epc_flag = 1
                  or mild_rise_streak_len >= 4
                then 'green'
    
                -- 🔥 RED : Material problems
                when low_streak_len >= 3
                  or sharp_drop_flag = 1
                  or low_epc_low_rev_flag = 1
                  or low_epc_high_rev_flag = 1
                  or low_epc_flag = 1
                  or mild_drift_streak_len >= 4
                  or volume_epc_stagnation_flag = 1
                then 'red'
    
                ELSE 'no impact'
            END AS alert_bucket
    
    FROM final
    WHERE Date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY Date DESC, Earnings DESC;
    """

    df = run_query(query)
    return df
