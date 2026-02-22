import time
start_time = time.time()

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import os
from datetime import datetime
import sys
import tempfile
import urllib.parse
from sqlalchemy import create_engine, text
from datetime import date, timedelta
import pytz
import warnings
import logging

os.environ["PYTHONWARNINGS"] = "ignore"

warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", message=".*No runtime found.*")
warnings.filterwarnings("ignore", message=".*ScriptRunContext.*")


# Add dashboard path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# --- CONFIGURATION ---
SMTP_HOST = "mail.dinerosoftware.com"
SMTP_PORT = 25
USERNAME = "smtpcheck@dinerosoftware.com"
PASSWORD = "SMTP801l0g1n"

TO_EMAILS = ["ryan@tapstone.com", "suphaus@tapstone.com", "jon@tapstone.com"]
CC_EMAILS = ["jatin@tapstone.com", "amit@tapstone.com", "monetization@tapstone.com", "datateam@dinerosoftware.com", "michelle@tapstone.com", "haley@tapstone.com"]

context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.set_ciphers("DEFAULT@SECLEVEL=1")
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE



# --- DB Connection --- #
def get_engine():
    
    port = 3306
    user = urllib.parse.quote_plus("jatin")
    password = urllib.parse.quote_plus("57nOthuwo*a4ep!E@Ru_R")
    host = urllib.parse.quote_plus("dataanalystdb.dinerotesting.com")
    database = urllib.parse.quote_plus("DataAnalyst_Jatin")

    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}?charset=utf8mb4", pool_pre_ping=True, pool_recycle=3600)
    return engine


def run_query(query):
    engine = get_engine()
    with engine.begin() as conn:
        try:
            return pd.read_sql(query, conn)
        except:
            conn.execute(text(query))
            return pd.DataFrame()


# --- DB Helpers --- #

def get_latest_event_date():    
    query = "SELECT MAX(eventDate) AS latest_date FROM team_block_stats"
    
    df = run_query(query)
    
    return df.iloc[0]["latest_date"] if not df.empty else None



def was_alert_sent(alert_date):
    query = f"""
        SELECT 1 
        FROM email_alerts_log 
        WHERE alert_date = '{alert_date}' AND email_type = 'alert_email'
        LIMIT 1
    """
    df = run_query(query)
    return not df.empty



def get_next_alert_date():
    """
    Returns the next alert_date that should be processed, based only on successful ALERT emails.
    """
    query = """
        SELECT DATE_ADD(MAX(alert_date), INTERVAL 1 DAY) AS next_date
        FROM email_alerts_log
        WHERE email_type = 'alert_email'
    """
    df = run_query(query)

    # First run ever (no rows in log)
    if df.empty or df.iloc[0]["next_date"] is None:
        latest_stats_date = get_latest_event_date()
        return latest_stats_date

    return df.iloc[0]["next_date"]



def is_data_available_for_date(alert_date):
    latest_stats_date = get_latest_event_date()
    return alert_date <= latest_stats_date



def was_no_data_email_sent(alert_date):
    query = f"""
        SELECT 1
        FROM email_alerts_log
        WHERE alert_date = '{alert_date}'
          AND email_type = 'no_data_email'
        LIMIT 1
    """
    df = run_query(query)
    return not df.empty


def log_email_alert(alert_date, red_df, green_df, partner_df, email_type, csv_paths):
    
    csv_paths = csv_paths if csv_paths is not None else []
    red_csv = None
    green_csv= None
    partner_csv = None
    
    for p in csv_paths:
        name = os.path.basename(p)
        if "RED_ALERTS" in name:
            red_csv = name
        elif "GREEN_ALERTS" in name:
            green_csv = name
        elif "Partner_Spikes" in name:
            partner_csv = name
    
    red_count   = len(red_df) if red_df is not None else "NULL"
    green_count = len(green_df) if green_df is not None else "NULL"
    partner_count = len(partner_df) if partner_df is not None else "NULL"
    csv_files_count = len(csv_paths) if email_type != "no_data_email" else "NULL"
    
    query = f"""
        INSERT INTO email_alerts_log
        (alert_date, email_type, red_alert_count, green_alert_count, partner_alert_count, csv_files_count,
         red_alert_csv, green_alert_csv, partner_spike_csv, sent_at)
        VALUES
        ('{alert_date}', '{email_type}', {red_count}, {green_count}, {partner_count}, {csv_files_count},
         {f"'{red_csv}'" if red_csv else "NULL"},
         {f"'{green_csv}'" if green_csv else "NULL"},
         {f"'{partner_csv}'" if partner_csv else "NULL"},
         CONVERT_TZ(NOW(), 'UTC', 'US/Pacific')
         )
        ON DUPLICATE KEY UPDATE
            red_alert_count = {red_count},
            green_alert_count = {green_count},
            partner_alert_count = {partner_count},
            csv_files_count = {csv_files_count},
            red_alert_csv = {f"'{red_csv}'" if red_csv else "NULL"},
            green_alert_csv = {f"'{green_csv}'" if green_csv else "NULL"},
            partner_spike_csv = {f"'{partner_csv}'" if partner_csv else "NULL"},
            sent_at = CONVERT_TZ(NOW(), 'UTC', 'US/Pacific');
    """
    run_query(query)




# --- EPC Tracker Query --- #


def fetch_epc_tracker(alert_date):

    # --------------------------------------------------
    # 1. STATIC DATE LOGIC (V1)
    # --------------------------------------------------
    #alert_date = get_latest_event_date()
    
    start_date = alert_date - timedelta(days=45)


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
        WHERE eventDate BETWEEN '{start_date}' AND '{alert_date}' and est_earnings > 5 and uniq_impr > 50 and partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
    ),

    rolling AS (
        SELECT
            b.*,
            SUM(est_earnings) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        / NULLIF(SUM(paid_clicks) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS epc_7d_avg,
            
            AVG(b.est_earnings) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS earn_7d_avg,
            
            LAG(b.epc) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate) AS prev_epc,

            avg(b.uniq_impr) over (partition by b.keyword_block_id order by b.eventDate rows between 6 preceding and current row) as impr_7d_avg
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
           
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epc < r.epc_7d_avg * 0.8) ORDER BY r.eventDate) AS 
              grp_low,

            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epc > r.epc_7d_avg * 1.15) ORDER BY r.eventDate) AS 
              grp_high,

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

        ROUND(((f.epc - f.epc_7d_avg) / NULLIF(f.epc_7d_avg, 0)) * 100, 1) AS epc_perf_pct,
        
        ROUND(((f.est_earnings - f.earn_7d_avg) / NULLIF(f.earn_7d_avg, 0)) * 100, 1) AS rev_perf_pct
            
        FROM flagged f
    ),

    global_block_revenue_share AS (
        SELECT
            t.eventDate, t.keyword_block_id,
            --SUM(t.est_earnings) * 1.0 / NULLIF(SUM(SUM(t.est_earnings)) OVER (), 0) AS global_revenue_share,
            (t.est_earnings * 1.0) / nullif(sum(sum(t.est_earnings)) over (partition by t.eventDate), 0) as daily_rev_share
        FROM team_block_stats t
        WHERE t.eventDate between '{start_date}' and '{alert_date}'
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
            s.uniq_impr AS Impressions,
            s.paid_clicks AS Clicks,
            ROUND(s.epc, 4) AS EPC,
            ROUND(s.epc_7d_avg, 4) AS `7D Avg EPC`,
            round(g.daily_rev_share * 100, 2) as `Block's Daily Share`,

            CASE
                -- 1) CRITICAL RED (streak / sharp drops)
                WHEN s.low_streak_len >= 3 AND g.daily_rev_share > 0.015 THEN '3D EPC Decline on High Revenue Block'
                WHEN s.low_streak_len >= 3 THEN '3D EPC Decline Streak'
                WHEN s.sharp_drop_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Sharp EPC Drop on High Revenue Block'
                WHEN s.sharp_drop_flag = 1 THEN 
                CONCAT('Sharp EPC Drop From Yesterday by ', ROUND(((s.epc - s.prev_epc) / NULLIF(s.prev_epc, 0)) * 100, 2), '%')

                -- 2) CRITICAL GREEN (streak / sharp rises)
                WHEN s.high_streak_len >= 3 AND g.daily_rev_share > 0.015 THEN '3D EPC Rise on High Revenue Block'
                WHEN s.high_streak_len >= 3 THEN '3D EPC Rise Streak'
                WHEN s.sharp_rise_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Sharp EPC Rise on High Revenue Block'
                WHEN s.sharp_rise_flag = 1 then 
                CONCAT('Sharp EPC Rise From Yesterday by ', ROUND(((s.epc - s.prev_epc) / NULLIF(s.prev_epc, 0)) * 100, 2), '%')

                -- 3) DAILY MOVERS

                WHEN s.mild_drift_streak_len >= 4 AND s.is_current_drop_streak_end = 1 
                    THEN concat('EPC Declining Daily For ', s.mild_drift_streak_len, ' Days')
                
                WHEN s.mild_rise_streak_len >= 4 AND s.is_current_rise_streak_end = 1 
                    THEN concat('EPC Rising Daily For ', s.mild_rise_streak_len, ' Days')

                -- 4) SINGLE-DAY diagnostics
                WHEN s.low_epc_low_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Both EPC & Rev Low on High Revenue Block'
                WHEN s.low_epc_high_rev_flag = 1 THEN CONCAT('EPC: ', s.epc_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')

                WHEN s.high_epc_high_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN 'EPC & Revenue Both High on High Revenue Block'
                WHEN s.high_epc_low_rev_flag = 1 THEN CONCAT('EPC: ', s.epc_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')
                WHEN s.high_epc_high_rev_flag = 1 THEN 'EPC & Revenue Both Higher Than 7D Avg'

                WHEN s.low_epc_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Low EPC - High Revenue Block'
                WHEN s.high_epc_flag = 1 AND g.daily_rev_share > 0.015 THEN 'High EPC - High Revenue Block'
                WHEN s.low_epc_flag = 1 THEN CONCAT('EPC DOWN (', ROUND(((s.epc - s.epc_7d_avg) / NULLIF(s.epc_7d_avg, 0)) * 100, 2), '%)')
                WHEN s.high_epc_flag = 1 THEN CONCAT('EPC UP (', ROUND(((s.epc - s.epc_7d_avg) / NULLIF(s.epc_7d_avg, 0)) * 100, 2), '%)')

                -- 🟡 SCALE QUALITY WATCH
            	WHEN s.volume_epc_stagnation_flag = 1 AND g.daily_rev_share > 0.01 THEN 'Traffic & Revenue Spike with Stable EPC on High Revenue Block'
                WHEN s.volume_epc_stagnation_flag = 1 THEN 'Traffic & Revenue Spike with Stable EPC'

                ELSE 'Within Thresholds'
                
            END AS Alerts,

            s.high_streak_len, s.sharp_rise_flag, s.high_epc_high_rev_flag, s.high_epc_low_rev_flag, s.high_epc_flag, s.mild_rise_streak_len,
            s.low_streak_len, s.sharp_drop_flag, s.low_epc_low_rev_flag, s.low_epc_high_rev_flag, s.low_epc_flag, s.mild_drift_streak_len,
            s.volume_epc_stagnation_flag
            
        FROM streaked s
        LEFT JOIN global_block_revenue_share g ON s.keyword_block_id = g.keyword_block_id AND s.eventDate = g.eventDate
    )

    SELECT 
        Date, `Block ID`,
    	Partner,
    	`Block Name`,
    	Earnings,
    	`7D Avg Earnings`,
    	Impressions,
    	Clicks,
    	EPC, 
        `7D Avg EPC`, `Block's Daily Share`,
        Alerts,

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
    -- WHERE `Alerts` != 'Within Thresholds'
    where Date = '{alert_date}' and Earnings > 50
    ORDER BY Earnings DESC;
    """

    df = run_query(query)
    return df




# --- EPI Tracker Query --- #


def fetch_epi_tracker(alert_date):

    # --------------------------------------------------
    # 1. STATIC DATE LOGIC (V1)
    # --------------------------------------------------
    #alert_date = get_latest_event_date()
    
    start_date = alert_date - timedelta(days=45)


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
        WHERE eventDate BETWEEN '{start_date}' AND '{alert_date}' and est_earnings > 5 and uniq_impr > 50 and partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
    ),

    rolling AS (
        SELECT
            b.*,
            SUM(est_earnings) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
            / NULLIF(SUM(uniq_impr) OVER (PARTITION BY keyword_block_id ORDER BY eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS epi_7d_avg,
            
            AVG(b.est_earnings) OVER (PARTITION BY b.keyword_block_id ORDER BY b.eventDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS earn_7d_avg,
            
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

            
            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epi < r.epi_7d_avg * 0.8) ORDER BY r.eventDate) AS 
              grp_low,

            ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id ORDER BY r.eventDate)
              - ROW_NUMBER() OVER (PARTITION BY r.keyword_block_id, (r.epi > r.epi_7d_avg * 1.15) ORDER BY r.eventDate) AS 
              grp_high,

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

        ROUND(((f.epi - f.epi_7d_avg) / NULLIF(f.epi_7d_avg, 0)) * 100, 1) AS epi_perf_pct,
        
        ROUND(((f.est_earnings - f.earn_7d_avg) / NULLIF(f.earn_7d_avg, 0)) * 100, 1) AS rev_perf_pct

            
        FROM flagged f
    ),

    global_block_revenue_share AS (
        SELECT
            t.eventDate, t.keyword_block_id,
            (t.est_earnings * 1.0) / nullif(sum(t.est_earnings) over (partition by t.eventDate), 0) as daily_rev_share
        FROM team_block_stats t
        WHERE t.eventDate between '{start_date}' and '{alert_date}'
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
            s.uniq_impr AS Impressions,
            s.paid_clicks AS Clicks,
            ROUND(s.epi, 4) AS EPI,
            ROUND(s.epi_7d_avg, 4) AS `7D Avg EPI`,
            round(g.daily_rev_share * 100, 2) as `Block's Daily Share`,


            CASE
                -- 1) CRITICAL RED (streak / sharp drops)
                WHEN s.low_streak_len >= 3 AND g.daily_rev_share > 0.015 THEN '3D EPI Decline in High Revenue Block'
                WHEN s.low_streak_len >= 3 THEN '3D EPI Decline Streak'
                WHEN s.sharp_drop_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Sharp EPI Drop on High Revenue Block'
                WHEN s.sharp_drop_flag = 1 THEN 
                CONCAT('Sharp EPI Drop From Yesterday by ', ROUND(((s.epi - s.prev_epi) / NULLIF(s.prev_epi, 0)) * 100, 2), '%')

                -- 2) CRITICAL GREEN (streak / sharp rises)
                WHEN s.high_streak_len >= 3 AND g.daily_rev_share > 0.015 THEN '3D EPI Rise in High Revenue Block'
                WHEN s.high_streak_len >= 3 THEN '3D EPI Rise Streak'
                WHEN s.sharp_rise_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Sharp EPI Rise on High Revenue Block'
                WHEN s.sharp_rise_flag = 1 THEN 
                CONCAT('Sharp EPI Rise From Yesterday by ', ROUND(((s.epi - s.prev_epi) / NULLIF(s.prev_epi, 0)) * 100, 2), '%')

                -- 3) DAILY MOVERS

                WHEN s.mild_drift_streak_len >= 4 AND s.is_current_drop_streak_end = 1 
                    THEN concat('EPI Declining Daily For ', s.mild_drift_streak_len, ' Days')
                
                WHEN s.mild_rise_streak_len >= 4 AND s.is_current_rise_streak_end = 1 
                    THEN concat('EPI Rising Daily For ', s.mild_rise_streak_len, ' Days')

                -- 4) SINGLE-DAY diagnostics
                WHEN s.low_epi_low_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Both EPI & Rev Low on High Revenue Block'
                when s.low_epi_high_rev_flag = 1 then CONCAT('EPI: ', s.epi_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')

                WHEN s.high_epi_high_rev_flag = 1 AND g.daily_rev_share > 0.015 THEN 'EPI & Revenue Both High on High Revenue Block'
                WHEN s.high_epi_low_rev_flag = 1 then CONCAT('EPI: ', s.epi_perf_pct, '% | Rev: ', s.rev_perf_pct, '%')
                WHEN s.high_epi_high_rev_flag = 1 THEN 'EPI & Revenue Both Higher Than 7D Avg'

                WHEN s.low_epi_flag = 1 AND g.daily_rev_share > 0.015 THEN 'Low EPI - High Revenue Block'
                WHEN s.high_epi_flag = 1 AND g.daily_rev_share > 0.015 THEN 'High EPI - High Revenue Block'
                WHEN s.low_epi_flag = 1 THEN CONCAT('EPI DOWN (', ROUND(((s.epi - s.epi_7d_avg) / NULLIF(s.epi_7d_avg, 0)) * 100, 2), '%)')
                WHEN s.high_epi_flag = 1 THEN CONCAT('EPI UP (', ROUND(((s.epi - s.epi_7d_avg) / NULLIF(s.epi_7d_avg, 0)) * 100, 2), '%)')

                -- 🟡 SCALE QUALITY WATCH
            	WHEN s.volume_epi_stagnation_flag = 1 AND g.daily_rev_share > 0.01 THEN 'Traffic & Revenue Spike with Stable EPI on High Revenue Block'
                WHEN s.volume_epi_stagnation_flag = 1 THEN 'Traffic & Revenue Spike with Stable EPI'
                

                ELSE 'Within Thresholds'
            END AS Alerts,

            s.high_streak_len, s.sharp_rise_flag, s.high_epi_high_rev_flag, s.high_epi_low_rev_flag, s.high_epi_flag, s.mild_rise_streak_len,
            s.low_streak_len, s.sharp_drop_flag, s.low_epi_low_rev_flag, s.low_epi_high_rev_flag, s.low_epi_flag, s.mild_drift_streak_len,
            s.volume_epi_stagnation_flag

        FROM streaked s
        LEFT JOIN global_block_revenue_share g ON s.keyword_block_id = g.keyword_block_id AND s.eventDate = g.eventDate

    )

    SELECT 
        Date, `Block ID`,
    	Partner,
    	`Block Name`,
    	Earnings,
    	`7D Avg Earnings`,
    	Impressions,
    	Clicks,
    	EPI, 
        `7D Avg EPI`,
        `Block's Daily Share`,
        Alerts,
        
        
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
    -- WHERE `Alerts` != 'Within Thresholds'
    where Date = '{alert_date}' and Earnings > 50
    ORDER BY Earnings DESC;
    """

    df = run_query(query)
    return df




# --- Partner Volume Spike Tracker Query --- #


def fetch_volume_spike_tracker(alert_date):
    #alert_date = get_latest_event_date()

    start_date = alert_date - timedelta(days=45)

    query = f"""
    WITH base AS (
        SELECT
            eventDate,
            partner,
            SUM(paid_clicks) AS paid_clicks,
            SUM(uniq_impr) AS uniq_impr,
            SUM(est_earnings) AS est_earnings
        FROM team_block_stats
        WHERE eventDate between '{start_date}' and '{alert_date}'
          #AND est_earnings > 5 and uniq_impr > 50
          and est_earnings > 0 and uniq_impr > 50
          AND partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
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

    
    metrics AS (
        SELECT
            r.*,
            paid_clicks * 1.0 / NULLIF(uniq_impr, 0) AS ctr,
            avg_clicks_7d * 1.0 / NULLIF(avg_impr_7d, 0) AS avg_ctr_7d,

            (paid_clicks  / NULLIF(avg_clicks_7d, 0)) - 1 AS click_pct,
            (uniq_impr    / NULLIF(avg_impr_7d, 0)) - 1 AS impr_pct,
            (est_earnings / NULLIF(avg_rev_7d, 0)) - 1 AS revenue_pct,

            -- CORRECTED: Partner's share of total revenue (daily)
            COALESCE(p.daily_rev_share, 0) AS daily_rev_share

        FROM rolling r
        LEFT JOIN partner_rev_share p ON r.partner = p.partner and r.eventDate = p.eventDate
    )
    
    SELECT
        eventDate AS Date, partner as Partner,

        ROUND(est_earnings, 2) AS Earnings,
        
        uniq_impr AS Impressions,
        
        paid_clicks AS Clicks,
        
        ROUND(ctr * 100, 2) AS CTR,

        #round(avg_impr_7d, 2) as Impr_7D, round(avg_clicks_7d, 2) as Clicks_7D, round(avg_rev_7d, 2) as Rev_7D, round(avg_ctr_7d * 100, 2) as CTR_7D,

        ROUND(click_pct * 100, 2) AS `Clicks vs 7D`,
        ROUND(impr_pct * 100, 2) AS `Impr vs 7D`,
        ROUND(revenue_pct * 100, 2) AS `Rev vs 7D`,

        ROUND(daily_rev_share * 100, 2) AS `Partner's Daily Share`,

        CASE
            -- Tier 1: Healthy
                WHEN impr_pct > 0.5 AND revenue_pct >= 0.2 AND ctr >= avg_ctr_7d * 0.9 THEN 'Strong Scale'
                WHEN impr_pct > 0.25 AND revenue_pct >= 0.1 AND ctr >= avg_ctr_7d * 0.7 THEN 'Healthy Growth'
            
            -- Tier 2: Warnings
                WHEN impr_pct < -0.2 and revenue_pct < -0.2 THEN 'Early Decline'  -- Gradual drops
                when impr_pct < -0.2 and revenue_pct > 0 then 'Traffic DOWN - Revenue UP' 
                WHEN impr_pct > 0.25 AND revenue_pct <= 0.1 THEN 'Suspicious Spike'
            
            -- Tier 3: Critical
                WHEN impr_pct > 1.0 AND ctr < avg_ctr_7d * 0.5 THEN 'Bot Surge'
                WHEN impr_pct < -0.4 AND revenue_pct < -0.4 THEN 'Sharp Drop'
                
                ELSE 'Stable'

        END AS Alerts
    
    FROM metrics
    having Date = '{alert_date}'
    and Alerts in ('Strong Scale', 'Suspicious Spike', 'Bot Surge', 'Traffic DOWN - Revenue UP', 'Early Decline')
    and Earnings > 50
     
    ORDER BY Date DESC, Earnings DESC;
    """
    
    df = run_query(query)
    
    return df



# --- Helper Functions --- #


def get_alerts(alert_date):
    
    print("🔄 Running dashboard trackers...")
    
    print(f"📤 Preparing alerts for {alert_date}")
    
    epc_df = fetch_epc_tracker(alert_date)
    epi_df = fetch_epi_tracker(alert_date)
    partner_df = fetch_volume_spike_tracker(alert_date)
    
    # -----------------------------
    # 1. Filter RED & GREEN alerts
    # -----------------------------
    
    red_epc = epc_df[epc_df['alert_bucket'] == 'red'].copy() if not epc_df.empty else pd.DataFrame()

    green_epc = epc_df[epc_df['alert_bucket'] == 'green'].copy() if not epc_df.empty else pd.DataFrame()
    
    red_epi = epi_df[epi_df['alert_bucket'] == 'red'].copy() if not epi_df.empty else pd.DataFrame()

    green_epi = epi_df[epi_df['alert_bucket'] == 'green'].copy() if not epi_df.empty else pd.DataFrame()

    # -----------------------------
    # 2. Normalize EPC schema
    # -----------------------------
    
    if not red_epc.empty:
        red_epc = red_epc.rename(columns={'EPC': 'Metric Value', '7D Avg EPC': '7D Avg Metric'})
        
        red_epc['Metric Type'] = 'EPC'


    if not green_epc.empty:
        green_epc = green_epc.rename(columns={'EPC': 'Metric Value', '7D Avg EPC': '7D Avg Metric'})

        green_epc['Metric Type'] = 'EPC'

    # -----------------------------
    # 3. Normalize EPI schema
    # -----------------------------
    
    if not red_epi.empty:
        red_epi = red_epi.rename(columns={'EPI': 'Metric Value', '7D Avg EPI': '7D Avg Metric'})
        
        red_epi['Metric Type'] = 'EPI'


    if not green_epi.empty:
        green_epi = green_epi.rename(columns={'EPI': 'Metric Value', '7D Avg EPI': '7D Avg Metric'})
        
        green_epi['Metric Type'] = 'EPI'


    # -----------------------------
    # 4. Select COMMON & REQUIRED columns only
    # -----------------------------
    
    common_cols = [
        'Block ID',
        'Partner',
        'Block Name',
        'Earnings',
        '7D Avg Earnings',
        'Impressions',
        'Clicks',
        'Metric Type',
        'Metric Value',
        '7D Avg Metric',
        "Block's Daily Share",
        'Alerts'
    ]

    red_epc = red_epc[common_cols] if not red_epc.empty else pd.DataFrame(columns=common_cols)
    red_epi = red_epi[common_cols] if not red_epi.empty else pd.DataFrame(columns=common_cols)

    green_epc = green_epc[common_cols] if not green_epc.empty else pd.DataFrame(columns=common_cols)
    green_epi = green_epi[common_cols] if not green_epi.empty else pd.DataFrame(columns=common_cols)

    
    # -----------------------------
    # 5. Combine safely
    # -----------------------------
    
    all_red = pd.concat([red_epc, red_epi], ignore_index=True)

    all_green = pd.concat([green_epc, green_epi], ignore_index=True)

    group_cols = [
        'Block ID',
        'Partner',
        'Block Name',
        'Earnings',
        '7D Avg Earnings',
        'Impressions',
        'Clicks',
        "Block's Daily Share"
    ]


    def combine_alerts(series):
        return ", ".join(series.dropna().unique())

    def combine_metric_types(series):
        return ", ".join(series.dropna().unique())
    
    def combine_metric_values(df):
        return ", ".join(
            f"{mt}={val:.4f}"
            for mt, val in zip(df["Metric Type"], df["Metric Value"])
        )
    
    def combine_avg_metric_values(df):
        return ", ".join(
            f"{mt}={val:.4f}"
            for mt, val in zip(df["Metric Type"], df["7D Avg Metric"])
        )
    
    all_red_combined = (
        all_red
        .groupby(group_cols, as_index=False)
        .apply(lambda g: pd.Series({
            "Metric Type": combine_metric_types(g["Metric Type"]),
            "Metric Value": combine_metric_values(g),
            "7D Avg Metric": combine_avg_metric_values(g),
            "Alerts": combine_alerts(g["Alerts"])
        }))
    )

    all_red_combined = all_red_combined.sort_values(by="Earnings", ascending=False).reset_index(drop=True)


    all_green_combined = (
        all_green
        .groupby(group_cols, as_index=False)
        .apply(lambda g: pd.Series({
            "Metric Type": combine_metric_types(g["Metric Type"]),
            "Metric Value": combine_metric_values(g),
            "7D Avg Metric": combine_avg_metric_values(g),
            "Alerts": combine_alerts(g["Alerts"])
        }))
    )

    all_green_combined = all_green_combined.sort_values(by="Earnings", ascending=False).reset_index(drop=True)
    
    
    partner_spikes = partner_df.copy() if not partner_df.empty else pd.DataFrame()

    print(f"📊 Found {len(all_red)} RED alerts & {len(all_green)} GREEN alerts for {alert_date}")

    return all_red, all_red_combined, all_green, all_green_combined, partner_spikes#, alert_date


def create_csv(red_alerts, green_alerts, partner_spikes, temp_dir, alert_date):
    paths = []
    
    if red_alerts.empty and partner_spikes.empty and green_alerts.empty:
        return None

    if not red_alerts.empty:
        red_path = os.path.join(temp_dir, f"RED_ALERTS_{alert_date.strftime('%Y%m%d')}.csv")
        red_alerts.to_csv(red_path, index=False)
        paths.append(red_path)

    if not green_alerts.empty:
        green_path = os.path.join(temp_dir, f"GREEN_ALERTS_{alert_date.strftime('%Y%m%d')}.csv")
        green_alerts.to_csv(green_path, index=False)
        paths.append(green_path)

    if not partner_spikes.empty:
        partner_path = os.path.join(temp_dir, f"Partner_Spikes_{alert_date.strftime('%Y%m%d')}.csv")
        partner_spikes.to_csv(partner_path, index=False)
        paths.append(partner_path)

    print(f"💾 Saved {len(paths)} CSV file(s)")
    
    return paths
    



# --- MAIN FUNCTION --- #


def main():
    print("🚀 Dashboard Alert Email Starting...")

    alert_date = get_next_alert_date()

    if alert_date is None:
        print("🛑 No valid alert date.")
        return

    # --------------------------
    # ⏰ TIME GATE
    # --------------------------
    pst_timezone = pytz.timezone('US/Pacific')
    pst_now = datetime.now(pst_timezone)

    if pst_now.hour < 10 or (pst_now.hour == 10 and pst_now.minute < 20):
        print(f"⏰ Too early to check for data. Current PST time: {pst_now.strftime('%H:%M')}. Exiting.")
        return

    # --------------------------
    # 📅 DATE GATE
    # --------------------------
    pst_today = pst_now.date()

    if alert_date >= pst_today:
        print(f"Alert_date {alert_date} is today or future. Data won't be available until tomorrow. Exiting.")
        return
    

    # --------------------------
    # 1️⃣ NO DATA CASE
    # --------------------------

    if not is_data_available_for_date(alert_date):

        print(f"⏸ No data available yet for {alert_date}")

        if was_no_data_email_sent(alert_date):
            print("📭 No-data email already sent. Skipping.")
            return

        print("📧 Sending NO DATA email...")
    
        subject = f"KPI Alerts – Data Pending for {alert_date}"

        pst_timezone = pytz.timezone('US/Pacific')
        pst_now = datetime.now(pst_timezone)

        body = f"""
            <html>
                <head>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            color: #333;
                        }}
                        .container {{
                            padding: 20px;
                            max-width: 600px;
                        }}
                        .notice {{
                            padding: 12px 16px;
                            background-color: #fff8e1;
                            border-left: 5px solid #f9a825;
                            margin: 20px 0;
                        }}
                        .footer {{
                            margin-top: 25px;
                            font-size: 12px;
                            color: #777;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <p>Hello Team,</p>
        
                        <div class="notice">
                            <strong>Data Pending:</strong> Stats for <strong>{alert_date}</strong> are not yet available in the system.
                        </div>
        
                        <p>
                            The KPI alert system ran successfully on schedule but found no data for <strong>{alert_date}</strong>. This is typically due to a delay in data ingestion.
                        </p>
        
                        <p>
                            No action is required — once data becomes available, the alert email for <strong>{alert_date}</strong> will be sent out automatically.
                        </p>
        
                        <p>
                            Access the live dashboard here:<br>
                            <a href="https://keywordtool.simpleadmin.io/Block_Alert_System">KPI Alert Console</a>
                        </p>

                        <p>
                            Thank you for your patience.
                        </p>
        
                        <div class="footer">
                            Generated on {pst_now.strftime('%Y-%m-%d %H:%M:%S PST')} | KPI Alert Console
                        </div>
                    </div>
                </body>
            </html>
            """
    
        try:
            msg = MIMEMultipart()
            msg["From"] = USERNAME
            msg["To"] = ", ".join(TO_EMAILS)
            msg["Cc"] = ", ".join(CC_EMAILS)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "html"))

            all_recipients = TO_EMAILS + CC_EMAILS
    
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=60)
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(USERNAME, PASSWORD)
            
            server.send_message(msg, from_addr=USERNAME, to_addrs=all_recipients)
            server.quit()
    
            print("✅ No Data email sent and marked!")

            try:
                log_email_alert(alert_date=alert_date,
                                red_df=None,
                                green_df=None,
                                partner_df=None,
                                email_type="no_data_email",
                                csv_paths=[]
                               )
                print(f"📝 Logged email alert for {alert_date}")
            except Exception as e:
                print("⚠️ Email sent BUT logging failed:", e)
    
        except Exception as e:
            print(f"❌ No-data email failed: {str(e)}")
        return


    # --------------------------
    # 2️⃣ ALERT ALREADY SENT?
    # --------------------------
    if was_alert_sent(alert_date):
        print(f"📛 Alert already sent for {alert_date}")
        return

    # --------------------------
    # 3️⃣ FETCH ALERTS
    # --------------------------
    
    # STEP 1: Get alerts
    all_red_alerts, combined_red_alerts, all_green_alerts, combined_green_alerts, partner_spikes = get_alerts(alert_date)
    
    # STEP 2: Create temp directory for attachments
    with tempfile.TemporaryDirectory() as temp_dir:
        attachments = []
        
        # STEP 3: Create CSV attachment if alerts exist
        if not combined_red_alerts.empty or not partner_spikes.empty or not combined_green_alerts.empty:
            
            csv_path = create_csv(combined_red_alerts, combined_green_alerts, partner_spikes, temp_dir, alert_date)
            
            if csv_path:
                attachments.extend(csv_path)
        else:
            print("ℹ️ No RED alerts or GREEN alerts or Partner spikes found. Skipping CSV generation.")
        
        # STEP 4: Email subject & contents
        
        subject = f"KPI Alerts – {alert_date}"

        unique_red_blocks = combined_red_alerts.drop_duplicates(subset=['Block ID'])

        unique_green_blocks = combined_green_alerts.drop_duplicates(subset=['Block ID'])

        unique_partners = partner_spikes['Partner'].nunique() if not partner_spikes.empty else 0
        
        total_red_blocks = len(unique_red_blocks)

        total_green_blocks = len(unique_green_blocks)
        
        total_red_revenue = unique_red_blocks['Earnings'].sum()

        total_green_revenue = unique_green_blocks['Earnings'].sum()

        pst_timezone = pytz.timezone('US/Pacific')
        
        pst_now = datetime.now(pst_timezone)


        html_body = f"""
            <html>
            <head>
                <style>
                body {{
                    font-family: Arial, sans-serif;
                    color: #333;
                }}
                .container {{
                    padding: 20px;
                    max-width: 600px;
                }}
                .alert-summary {{
                    margin: 20px 0;
                }}
                .red-kpi {{
                    padding: 12px;
                    background-color: #fff5f5;
                    border-left: 5px solid #d32f2f;
                    margin-bottom: 10px;
                }}
                .green-kpi {{
                    padding: 12px;
                    background-color: #f1f8e9;
                    border-left: 5px solid #689f38;
                }}
                .footer {{
                    margin-top: 25px;
                    font-size: 12px;
                    color: #777;
                }}
                </style>
            </head>
            
            <body>
                <div class="container">
                
                    <p>Hello Team,</p>
                    
                    <div class="alert-summary">
                    
                        <p><strong>Daily KPI Alert Summary for {alert_date}:</strong></p>
                        <ul>
                            <li><strong>{len(all_red_alerts)} <span style="color: #d32f2f;">Red alerts</span></strong> detected</li>
                            <li><strong>{len(all_green_alerts)} <span style="color: #689f38;">Green alerts</span></strong> detected</li>
                            <li><strong>{unique_partners}</strong> partner(s) with volume anomalies</li>
                        </ul>
                    </div>
                
                    <div class="red-kpi">
                        <strong>Red Alert Highlights:</strong><br>
                        • <strong>{total_red_blocks}</strong> unique blocks affected<br>
                        • <strong>${total_red_revenue:,.2f}</strong> revenue impacted
                    </div>
                    
                    <div class="green-kpi">
                        <strong>Green Alert Highlights:</strong><br>
                        • <strong>{total_green_blocks}</strong> unique blocks performing well<br>
                        • <strong>${total_green_revenue:,.2f}</strong> positive revenue contribution
                    </div>
                
                    <p>
                    Detailed block-level and partner-level diagnostics are attached for immediate review.
                    </p>
                    
                    <p>
                        Access the live dashboard here:<br>
                        <a href="https://keywordtool.simpleadmin.io/Block_Alert_System">
                        KPI Alert Console
                        </a>
                    </p>
                    
                    <div class="footer">
                        Generated on {pst_now.strftime('%Y-%m-%d %H:%M:%S PST')}<br>
                        KPI Alert Console
                    </div>
                </div>
            </body>
            </html>
            """
        try:
            msg = MIMEMultipart()
            msg["From"] = USERNAME
            msg["To"] = ", ".join(TO_EMAILS)
            msg["Cc"] = ", ".join(CC_EMAILS)
            msg["Subject"] = subject
            
            msg.attach(MIMEText(html_body, "html"))
    
            # Attach CSV if exists
            for file_path in attachments:
                with open(file_path, "rb") as f:
                    part = MIMEText(f.read().decode("utf-8"), "csv")
                    part.add_header(
                        "Content-Disposition",
                        f'attachment; filename="{os.path.basename(file_path)}"',
                    )
                    msg.attach(part)
    
            all_recipients = TO_EMAILS + CC_EMAILS
            
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=60)
            server.ehlo()
                
            print("🔐 Starting TLS handshake...")
            server.starttls(context=context)
            server.ehlo()
                
            server.login(USERNAME, PASSWORD)
            server.send_message(msg, from_addr=USERNAME, to_addrs=all_recipients)
            server.quit()

            try:
                log_email_alert(alert_date=alert_date, 
                                red_df=all_red_alerts, 
                                green_df=all_green_alerts, 
                                partner_df=partner_spikes,
                                email_type='alert_email',
                                csv_paths=attachments)
                print(f"📝 Logged email alert for {alert_date}")
            except Exception as e:
                print("⚠️ Email sent BUT logging failed:", e)
    
            print(f"✅ SUCCESS! Email sent to {len(all_recipients)} execs")

        except Exception as e:
            print(f"❌ Email failed: {str(e)}")

        finally:
            end_time = time.time()
            print(f"⏱ Total execution time: {end_time - start_time:.2f} seconds")


MAX_RETRIES = 3
RETRY_DELAY = 60

if __name__ == "__main__":
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"▶ Attempt {attempt} of {MAX_RETRIES}")
            main()
            print("✅ Script completed successfully")
            break
        except Exception as e:
            print(f"❌ Script failed on attempt {attempt}: {e}")

            if attempt < MAX_RETRIES:
                print(f"🔁 Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print("🛑 All retry attempts failed. Giving up.")
