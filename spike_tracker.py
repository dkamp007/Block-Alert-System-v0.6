import streamlit as st
import pandas as pd
from utils.db import run_query
from queries.block_details import get_latest_event_date
from datetime import date, timedelta



# --------------------------------------------------
# Partner Spike Tracker
# --------------------------------------------------



@st.cache_data(ttl=3600)
def fetch_volume_spike_tracker(partners=None):
    end_date = get_latest_event_date()

    start_date = end_date - timedelta(days=45)
    
    partner_filter_sql = ""
    if partners:
        partner_list = "', '".join(partners)
        partner_filter_sql = f"AND partner IN ('{partner_list}')"

    query = f"""
    WITH base AS (
        SELECT
            eventDate,
            partner,
            SUM(paid_clicks) AS paid_clicks,
            SUM(uniq_impr) AS uniq_impr,
            SUM(est_earnings) AS est_earnings
        FROM team_block_stats
        WHERE eventDate between '{start_date}' and '{end_date}'
          #AND est_earnings > 5 and uniq_impr > 50
          and est_earnings > 0 and uniq_impr > 50
          AND partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
          {partner_filter_sql}
        GROUP BY eventDate, partner
    ),

    rolling AS (
        SELECT
            *,
            AVG(paid_clicks)  OVER (PARTITION BY partner ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_clicks_7d,
            AVG(uniq_impr)    OVER (PARTITION BY partner ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_impr_7d,
            AVG(est_earnings) OVER (PARTITION BY partner ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_rev_7d
        FROM base
    ),

    partner_rev_share AS (
        SELECT 
            eventDate, 
            partner,
            (est_earnings * 1.0) / NULLIF(SUM(est_earnings) OVER (PARTITION BY eventDate), 0) AS daily_rev_share
        FROM base
    ),

    category_counts AS (
        SELECT
            eventDate,
            partner,
            COUNT(DISTINCT block_name) AS live_categories
        FROM team_block_stats
        WHERE eventDate between '{start_date}' and '{end_date}'
          AND uniq_impr > 10 and est_earnings > 0
          AND partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
          {partner_filter_sql}
        GROUP BY eventDate, partner
    ),
    
    
    metrics AS (
        SELECT
            r.*,
            c.live_categories,
            paid_clicks * 1.0 / NULLIF(uniq_impr, 0) AS ctr,
            avg_clicks_7d * 1.0 / NULLIF(avg_impr_7d, 0) AS avg_ctr_7d,

            (paid_clicks  / NULLIF(avg_clicks_7d, 0)) - 1 AS click_pct,
            (uniq_impr    / NULLIF(avg_impr_7d, 0)) - 1 AS impr_pct,
            (est_earnings / NULLIF(avg_rev_7d, 0)) - 1 AS revenue_pct,

            -- CORRECTED: Partner's share of total revenue (daily)
            COALESCE(p.daily_rev_share, 0) AS daily_rev_share

        FROM rolling r
        LEFT JOIN partner_rev_share p ON r.partner = p.partner and r.eventDate = p.eventDate
        LEFT JOIN category_counts c ON r.partner = c.partner and r.eventDate = c.eventDate
    )
    
    SELECT
        eventDate AS Date, partner as Partner,

        #round(avg_impr_7d, 2) as Impr_7D, round(avg_clicks_7d, 2) as Clicks_7D, round(avg_rev_7d, 2) as Rev_7D, round(avg_ctr_7d * 100, 2) as CTR_7D,

        CASE
            -- Tier 1: Healthy
                WHEN impr_pct > 0.5 AND revenue_pct >= 0.2 AND ctr >= avg_ctr_7d * 0.9 THEN '🚀 Strong Scale'
                WHEN impr_pct > 0.25 AND revenue_pct >= 0.1 AND ctr >= avg_ctr_7d * 0.7 THEN '✅ Healthy Growth'
            
            -- Tier 2: Warnings
                WHEN impr_pct < -0.2 and revenue_pct < -0.2 THEN '📉 Early Decline'  -- Gradual drops
                when impr_pct < -0.2 and revenue_pct > 0 then '🔍 Traffic DOWN - Revenue UP' 
                WHEN impr_pct > 0.25 AND revenue_pct <= 0.1 THEN '⚠️ Suspicious Spike'
            
            -- Tier 3: Critical
                WHEN impr_pct > 1.0 AND ctr < avg_ctr_7d * 0.5 THEN '☠️ Bot Surge'
                WHEN impr_pct < -0.4 AND revenue_pct < -0.4 THEN '📉 Sharp Drop'
                
                ELSE '➡️ Stable'

        END AS Alerts,

        ROUND(est_earnings, 2) AS Earnings, live_categories as `Live Categories`,
        
        uniq_impr AS Impressions,
        
        paid_clicks AS Clicks,
        
        ROUND(ctr * 100, 2) AS CTR,

        ROUND(click_pct * 100, 2) AS `Clicks vs 7D`,
        ROUND(impr_pct * 100, 2) AS `Impr vs 7D`,
        ROUND(revenue_pct * 100, 2) AS `Rev vs 7D`,

        ROUND(daily_rev_share * 100, 2) AS `Partner's Daily Share`
    
    FROM metrics
    ORDER BY Date DESC, Earnings DESC;
    """
    
    df = run_query(query)
    
    return df





# --------------------------------------------------
# Category Spike Tracker
# --------------------------------------------------




@st.cache_data(ttl=3600)
def fetch_category_spike_tracker(block_names=None, block_ids=None):
    end_date = get_latest_event_date()

    start_date = end_date - timedelta(days=45)

    conditions = [
        f"eventDate between '{start_date}' and '{end_date}'",
        "est_earnings > 5 and uniq_impr > 50",
        "partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')",
    ]
    
    #block_name_filter_sql = ""
    if block_names:
        block_names_list = "', '".join(block_names)
        conditions.append(f"block_name IN ('{block_names_list}')")

    #block_id_filter_sql = ""
    if block_ids:
        block_ids_list = ", ".join(map(str, block_ids))
        conditions.append(f"keyword_block_id in ({block_ids_list})")


    where_clause = " and ".join(conditions)

    query = f"""
    WITH base AS (
        SELECT
            eventDate, keyword_block_id, partner, block_name, est_earnings, uniq_impr, paid_clicks, epc, epi, ctr
        FROM team_block_stats
        WHERE {where_clause}
    ),

    rolling AS (
        SELECT
            *,
            AVG(paid_clicks)  OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_clicks_7d,
            AVG(uniq_impr)    OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_impr_7d,
            AVG(est_earnings) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_rev_7d
        FROM base
    ),

    -- 45-day revenue share across ALL categories
    block_rev_share AS (
        SELECT 
            keyword_block_id,
            SUM(est_earnings) * 1.0 / SUM(SUM(est_earnings)) OVER () AS revenue_share_45d
        FROM team_block_stats
        WHERE eventDate between '{start_date}' and '{end_date}'
          AND partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
          and est_earnings > 5 and uniq_impr > 50
        GROUP BY keyword_block_id
    ),

    
    metrics AS (
        SELECT
            r.*,
            
            (est_earnings * 1.0) / nullif(sum(est_earnings) over (partition by eventDate), 0) as daily_rev_share,
            
            avg_clicks_7d * 1.0 / NULLIF(avg_impr_7d, 0) AS avg_ctr_7d,

            (paid_clicks  / NULLIF(avg_clicks_7d, 0)) - 1 AS click_pct,
            (uniq_impr    / NULLIF(avg_impr_7d, 0)) - 1 AS impr_pct,
            (est_earnings / NULLIF(avg_rev_7d, 0)) - 1 AS revenue_pct,

            -- CORRECTED: block's share of total revenue (last 45 days)
            COALESCE(p.revenue_share_45d, 0) AS revenue_share_45d

        FROM rolling r
        
        LEFT JOIN block_rev_share p ON r.keyword_block_id = p.keyword_block_id
)
    
    SELECT
        eventDate AS Date, 
        keyword_block_id as `Block ID`, 

        CASE
            -- Tier 1: Healthy
                WHEN impr_pct > 0.5 AND revenue_pct >= 0.2 AND ctr >= avg_ctr_7d * 0.9 THEN '🚀 Strong Scale'
                WHEN impr_pct > 0.25 AND revenue_pct >= 0.1 AND ctr >= avg_ctr_7d * 0.7 THEN '✅ Healthy Growth'
            
            -- Tier 2: Warnings
                WHEN impr_pct < -0.2 and revenue_pct < -0.2 THEN '📉 Early Decline'  -- Gradual drops
                when impr_pct < -0.2 and revenue_pct > 0 then '🔍 Traffic DOWN - Revenue UP' 
                WHEN impr_pct > 0.25 AND revenue_pct <= 0.1 THEN '⚠️ Suspicious Spike'
            
            -- Tier 3: Critical
                WHEN impr_pct > 1.0 AND ctr < avg_ctr_7d * 0.5 THEN '☠️ Bot Surge'
                WHEN impr_pct < -0.4 AND revenue_pct < -0.4 THEN '📉 Sharp Drop'
                
                ELSE '➡️ Stable'

        END AS Alerts,

        partner as Partner,
        block_name as `Block Name`, 
        ROUND(est_earnings, 2) AS Earnings, 
        uniq_impr AS Impressions, 
        paid_clicks AS Clicks,
        
        ROUND(ctr, 2) AS CTR,

        ROUND(click_pct * 100, 2) AS `Clicks vs 7D`,
        ROUND(impr_pct * 100, 2) AS `Impr vs 7D`,
        ROUND(revenue_pct * 100, 2) AS `Rev vs 7D`,

        ROUND(revenue_share_45d * 100, 2) AS `Block's 45D Share`,
        round(daily_rev_share * 100, 2) as `Block's Daily Share`
    
    FROM metrics
    ORDER BY Date DESC, Earnings DESC;
    """
    
    df = run_query(query)
    
    return df
