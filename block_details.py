import streamlit as st
import pandas as pd
import numpy as np
from datetime import timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.db import run_query


# --------------------------------------------------
# CORE DATA FETCHERS
# --------------------------------------------------

@st.cache_data(ttl=3600)
def fetch_complete_mapping():
    """Fetch partner/block mapping for filters"""
    query = """
    SELECT DISTINCT partner, keyword_block_id, block_name 
    FROM team_block_stats
    WHERE partner NOT IN ('DIN', 'TWS', 'XYZ', 'XXX')
    """
    return run_query(query)


#@st.cache_data(ttl=3600)
def get_latest_event_date():
    """Get most recent event date"""
    query = "SELECT MAX(eventDate) AS latest_date FROM team_block_stats"
    df = run_query(query)
    return df.iloc[0]["latest_date"] if not df.empty else None




@st.cache_data(ttl=3600)
def fetch_system_stats():

    date = get_latest_event_date()
    
    query = f"""
    select 
        eventDate as Date, 
        round(sum(est_earnings), 2) as Earnings, 
        sum(uniq_impr) as Impressions, 
        round(sum(paid_clicks), 0) as Clicks, 
        round(sum(est_earnings)/sum(uniq_impr), 4) as EPI, 
        round(sum(est_earnings)/sum(paid_clicks), 4) as EPC, 
        round((sum(paid_clicks)/sum(uniq_impr)) * 100, 2) as CTR
    from team_block_stats
    where eventDate >= '{date}' - interval 59 day and partner not in ('DIN', 'TWS', 'XYZ', 'XXX') and est_earnings > 0
    group by eventDate
    order by eventDate desc;
    """

    return run_query(query)



@st.cache_data(ttl=3600)
def fetch_block_history(block_id):
    """Fetch 45-day history for deep dive analysis"""
    query = f"""
    SELECT 
        eventDate as Date, 
        epc as EPC, 
        epi as EPI,
        uniq_impr as Impressions, 
        paid_clicks as Clicks,
        est_earnings as Revenue
    FROM team_block_stats
    WHERE keyword_block_id = '{block_id}'
      AND eventDate >= (SELECT MAX(eventDate) FROM team_block_stats) - INTERVAL 45 DAY
    ORDER BY eventDate DESC
    """
    return run_query(query)


    
# @st.cache_data(ttl=3600)
# def fetch_partner_block_history(partner, date):
#     """Fetch partner blocks for specific date"""
#     query = f"""
#     SELECT 
#         eventDate as Date, block_name as `Block Name`,
#         est_earnings as Revenue, uniq_impr as Impressions,
#         paid_clicks as Clicks, epc as EPC, epi as EPI
#     FROM team_block_stats
#     WHERE partner = '{partner}' AND eventDate = '{date}' AND est_earnings > 0
#     ORDER BY uniq_impr DESC
#     """
#     return run_query(query)




@st.cache_data(ttl=3600)
def fetch_partner_category_snapshot(partner, date):
    query = f"""
    SELECT 
        eventDate, block_name as 'Block Name',
        uniq_impr AS Impressions,
        paid_clicks AS Clicks,
        est_earnings AS Earnings
    FROM team_block_stats
    WHERE partner = '{partner}' AND eventDate = '{date}' and uniq_impr > 0
    #GROUP BY eventDate, block_name
    order by est_earnings desc;
    """
    return run_query(query)




@st.cache_data(ttl=3600)
def fetch_partner_category_trend(partner):#, end_date):
    query = f"""
    SELECT 
        eventDate AS Date,
        COUNT(DISTINCT block_name) AS `Live Categories`,
        SUM(est_earnings) AS Earnings
    FROM team_block_stats
    WHERE partner = '{partner}'
      AND eventDate >= (SELECT MAX(eventDate) FROM team_block_stats) - INTERVAL 45 DAY
      and uniq_impr > 0 #and est_earnings > 0
    GROUP BY eventDate 
    ORDER BY eventDate desc;
    """
    
    return run_query(query)





# --------------------------------------------------
# DATA FILTERING UTILITIES
# --------------------------------------------------

def apply_display_days(df, date_col="Date", days=1, date_range=None):
    """Filter dataframe by display window or custom date range"""
    dff = df.copy()
    #dff[date_col] = pd.to_datetime(dff[date_col], errors="coerce").dt.normalize()
    
    if date_range is not None and len(date_range) > 0:
        #start_dt = pd.Timestamp(date_range[0]).normalize()
        start_dt = date_range[0]
        if len(date_range) == 2:
            #end_dt = pd.Timestamp(date_range[1]).normalize()
            end_dt = date_range[1]
            return dff[dff[date_col].between(start_dt, end_dt, inclusive="both")]
        return dff[dff[date_col] == start_dt]
    
    # Default: last N days
    max_dt = dff[date_col].max()
    cutoff = max_dt - pd.Timedelta(days=days - 1)
    return dff[dff[date_col] >= cutoff]


def apply_user_filters(df, partners=None, blocks=None, ids=None):
    """Apply user-side partner/block filters"""
    filtered = df.copy()
    
    if partners:
        filtered = filtered[filtered["Partner"].isin(partners)]
    
    if blocks:
        filtered = filtered[filtered["Block Name"].isin(blocks)]
    
    if ids:
        filtered = filtered[filtered["Block ID"].isin(ids)]
    
    return filtered

# --------------------------------------------------
# UI COMPONENTS
# --------------------------------------------------

def render_sidebar(mapping_df, latest_date):
    """Render filter sidebar"""
    with st.sidebar:
        st.header("🔍 Filters")
        st.markdown(f"📅 Latest: **{latest_date}**")
        st.markdown(f"📅 Data fetched for 45 days from **{latest_date}**")
        
        # Display window selector
        window_options = st.selectbox(
            "**Display Window**", 
            ['1d', '3d', '7d', '15d', '30d', 'Custom'],
            index=0,
            key='window_option'
        )
        
        display_days = None
        custom_range = None
        
        if window_options == 'Custom':
            custom_range = st.date_input(
                "Custom Range", 
                value=(latest_date - timedelta(days=6), latest_date),
                min_value=latest_date - timedelta(days=44),
                max_value=latest_date,
                key='custom_range'
            )
        else:
            display_days = int(window_options[:-1])
        
        # Partner/Block cascading filters
        partners = sorted(mapping_df["partner"].unique())
        selected_partners = st.multiselect("**👥 Partner**", partners)
        
        filtered = mapping_df.copy()
        if selected_partners:
            filtered = filtered[filtered["partner"].isin(selected_partners)]
        
        blocks = sorted(filtered["block_name"].unique())
        selected_blocks = st.multiselect("**🧱 Block Name**", blocks)
        
        if selected_blocks:
            filtered = filtered[filtered["block_name"].isin(selected_blocks)]
        
        block_ids = sorted(filtered["keyword_block_id"].unique())
        selected_ids = st.multiselect("**🆔 Block ID**", block_ids)

        #min_earnings = st.slider("**💰 Min Earnings Threshold ($)**", min_value=0.0, max_value=50.0, value=5.0, step=0.5)
                                 #help="Only show blocks with earnings > this value")
        
        run_btn = st.button("🚀 Run Tracker", type="primary", width='stretch')#, use_container_width=True)
    
    return selected_partners, selected_ids, selected_blocks, run_btn, display_days, custom_range#, min_earnings


# --------------------------------------------------
# UI COMPONENTS
# --------------------------------------------------


def render_kpis(raw_df, filtered_df=None, alert_view='RED'):
    
    display_df = filtered_df if filtered_df is not None else df
    
    alerts_df = display_df[display_df["Alerts"] != "Within Thresholds"]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🚨 Total Alerts", len(alerts_df))
    
    with col2:
        # high_rev_count = alerts_df["Alerts"].str.contains("High Revenue", na=False).sum()
        high_rev_count = int(alerts_df["is_high_revenue_block"].sum())
        st.metric("💰 High Revenue Alerts", high_rev_count)
    
    with col3:
        critical_count = alerts_df["Alerts"].str.contains("🔥|🚨|❌|⚠️", na=False).sum()
        st.metric("🔥 Critical Alerts", critical_count)
    
    with col4:
        # red_alerts = alerts_df[alerts_df['alert_bucket'] == 'red']
        if alert_view == 'RED' or alert_view == 'ALL':
            red_alerts = raw_df[raw_df['alert_bucket'] == 'red']
            rev_impact = red_alerts['Earnings'].sum()
            #st.metric('💸 Revenue Impacted', rev_impact if rev_impact > 0 else 0, format='dollar')
            st.metric('💸 Revenue Impacted', rev_impact, format='dollar')
        else:
            green_alerts = raw_df[raw_df['alert_bucket'] == 'green']
            rev_impact = green_alerts['Earnings'].sum()
            #st.metric('💸 Revenue Impacted', rev_impact if rev_impact > 0 else 0, format='dollar')
            st.metric('💸 Opportunity Revenue', rev_impact, format='dollar')




# --------------------------------------------------
# DATA TABLE RENDERERS
# --------------------------------------------------



def render_alert_table(df, column_config=None):
    
    st.dataframe(df, column_config=column_config or {}, width='stretch', hide_index=True)


def render_alert_table_volume(df, column_config=None, selectable=False, selection_mode="single-row", key=None):
    
    return st.dataframe(
        df, 
        column_config=column_config or {}, 
        width='stretch', 
        hide_index=True,
        on_select="rerun" if selectable else None,
        selection_mode=selection_mode if selectable else None,
        key=key
    )


# --------------------------------------------------
# DEEP DIVE CHARTS
# --------------------------------------------------

def render_performance_corridor(df, metric):
    """
    Renders a System Health Chart with a 'Normal' corridor (Confidence Band).
    If the metric breaks the corridor, it signals a significant anomaly.
    """
    # 1. Prepare Data (Ensure ascending order for rolling calcs)
    #df = df.sort_values("eventDate").copy()
    
    # 2. Calculate the Corridor (7-day window)
    window = 7
    # df['mean'] = df['Earnings'].rolling(window=window).mean()
    # df['std'] = df['Earnings'].rolling(window=window).std()

    df['mean'] = df[metric].rolling(window=window).mean()
    df['std'] = df[metric].rolling(window=window).std()
    
    # Upper and Lower Bounds (1.5 STD covers ~85% of 'normal' movement)
    df['upper'] = df['mean'] + (1.5 * df['std'])
    df['lower'] = df['mean'] - (1.5 * df['std'])

    st.markdown("##### 🛡️ System KPI Corridor")
    st.caption("The shaded area represents the 'Normal' performance range based on a 7-day rolling window")

    fig = go.Figure()

    # --- The Corridor (Shaded Area) ---
    # Add Upper Line (invisible)
    fig.add_trace(go.Scatter(
        x=df['Date'], y=df['upper'],
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))

    # Add Lower Line and Fill to Upper
    fig.add_trace(go.Scatter(
        x=df['Date'], y=df['lower'],
        fill='tonexty', # Fills the gap between upper and lower
        fillcolor='rgba(100, 116, 139, 0.15)', # Subtle slate grey
        line=dict(width=0),
        name='Normal Range',
        hoverinfo='skip'
    ))

    # --- The Baseline (Moving Average) ---
    fig.add_trace(go.Scatter(
        x=df['Date'], y=df['mean'],
        line=dict(color='rgba(59, 130, 246, 0.5)', width=2, dash='dot'),
        name='7D Baseline'
    ))

    # --- The Actual Performance (The Signal) ---
    fig.add_trace(go.Scatter(
        x=df['Date'], y=df[metric],# y=df['Earnings'],
        line=dict(color='#2563eb', width=3),
        name=f'Actual {metric}',
        mode='lines+markers'
    ))

    # UI/UX Layout Enhancements
    fig.update_layout(
        hovermode="x unified",
        template="plotly_white",
        height=300,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(showgrid=False),
        yaxis=dict(
            #title="Earnings ($)",
            title=metric,
            tickformat="$,.2f" if metric == 'Earnings' else "$,.4f",
            showgrid=True
            #gridcolor='rgba(0,0,0,0.05)'
        ),
        margin=dict(l=20, r=20, t=40, b=20)
    )

    st.plotly_chart(fig, width='stretch')




def render_impact_scatter(df):

    # 1. Filter for the latest date in the dataset to avoid over-plotting history
    # The SQL returns 45 days of data, but the scatter should show the "Current" state
    latest_date = df['Date'].max()
    current_df = df[df['Date'] == latest_date].copy()

    plot_df = current_df[~current_df['Alerts'].isin(['➡️ Stable', '✅ Healthy Growth'])].copy()
    
    if plot_df.empty:
        st.info("No significant partner movements to display in the Impact Map.")
        return

    # 2. Map colors to your specific SQL Alert Tiers
    color_map = {
        '🚀 Strong Scale': '#10b982',             # Emerald Green
        #'✅ Healthy Growth': '#34d399',          # Soft Green
        #'➡️ Stable': '#94a3b8',                  # Slate Grey
        '⚠️ Suspicious Spike': '#f59e0b',         # Amber
        '☠️ Bot Surge': '#7f1d1d',                # Dark Red
        '📉 Sharp Drop': '#f87171',               # Bright Red
        '📉 Early Decline': '#fa1414',            # Light Red
        '🔍 Traffic DOWN - Revenue UP': '#0011fc' # Blue
    }
        
    st.markdown(f"##### 🎯 Partner Impact Map")# {latest_date}")
    st.caption("Partner volume shifts vs. revenue impact")

    # 3. RENDER PLOT

    fig = px.scatter(
        plot_df,
        x="Impr vs 7D",
        y="Rev vs 7D",
        size="Earnings",
        color="Alerts",
        hover_name="Partner",
        color_discrete_map=color_map,
        custom_data=["Earnings", "Impr vs 7D", "Rev vs 7D", "CTR"]
    )

    # 4. Add Crosshairs at Zero
    fig.add_hline(y=0, line_dash="dot", line_color="#cbd5e1", line_width=2)
    fig.add_vline(x=0, line_dash="dot", line_color="#cbd5e1", line_width=2)

    # 5. Add Quadrant Action Labels
    # fig.add_annotation(x=current_df["Impr vs 7D"].max(), y=current_df["Rev_Change_Pct"].max(), 
    #                    text="SCALE", showarrow=False, opacity=0.5, font=dict(size=12))
    # fig.add_annotation(x=current_df["Impr vs 7D"].max(), y=current_df["Rev_Change_Pct"].min(), 
    #                    text="INVESTIGATE", showarrow=False, opacity=0.5, font=dict(size=12))

    # 6. Refine Tooltips
    fig.update_traces(
        hovertemplate="<br>".join([
            "<b>%{hovertext}</b>",
            "Earnings: $%{customdata[0]:,.2f}",
            "CTR: %{customdata[3]:.2f}%",
            "Volume Shift: %{customdata[1]:.1f}%",
            "Revenue Shift: %{customdata[2]:.1f}%"
        ])
    )

    fig.update_layout(
        height=300,
        template="plotly_white",
        xaxis=dict(title="Volume Change % (Traffic)", zeroline=False, ticksuffix="%"),
        yaxis=dict(title="Revenue Change % (Value)", zeroline=False, ticksuffix="%"),
        showlegend=False,
        #legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=30, b=10)
    )    

    st.plotly_chart(fig, width='stretch')






# def render_impact_scatter(df, metric_type="EPC"):
#     """
#     Renders a Quadrant-based Scatter Plot to identify high-priority partners.
#     X-axis: Volume Change %
#     Y-axis: Efficiency (EPC/EPI) Change %
#     Size: Current Earnings
#     """
#     # Map the dynamic metric
#     y_col = f"{metric_type} vs 7D"
    
#     # Filter out 'no impact' to keep the chart focused on movers
#     plot_df = df[df['alert_bucket'] != 'no impact'].copy()
    
#     if plot_df.empty:
#         st.info("No significant partner movements to display in the Impact Map.")
#         return

#     # 2. CALCULATE VARIANCES (Fixes the ValueError)
#     # Volume Change %
#     avg_impr_col = "7D Avg Impressions" if "7D Avg Impressions" in plot_df.columns else "Impressions" # Fallback

#     plot_df["Impr vs 7D"] = ((plot_df["Impressions"] / plot_df[avg_impr_col]) - 1) * 100
    
#     # Efficiency Change % (Y-Axis)
#     y_val_col = metric_type
#     y_avg_col = f"7D Avg {metric_type}"
#     y_col_name = f"{metric_type} vs 7D"
    
#     plot_df[y_col_name] = ((plot_df[y_val_col] / plot_df[y_avg_col]) - 1) * 100

#     st.markdown("##### 🎯 Partner Impact Map")
#     st.caption("Identifies the biggest movers in the system. **Red** bubbles represent alerts requiring immediate investigation")

#     # 3. RENDER PLOT
#     fig = px.scatter(
#         plot_df,
#         x="Impr vs 7D",
#         y=y_col_name,
#         size="Earnings",
#         color="alert_bucket",
#         hover_name="Partner" if "Partner" in plot_df.columns else "Block Name",
#         color_discrete_map={'red': '#ef4444', 'green': '#10b981'},
#         custom_data=["Earnings", "Impr vs 7D", y_col_name]
#         #title=f"Partner Velocity: Volume vs. {metric_type}"
#     )


#     # 3.5 Add Quadrant Lines (The Crosshair)
#     fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8", line_width=1.5, opacity=0.8)
#     fig.add_vline(x=0, line_dash="dash", line_color="#94a3b8", line_width=1.5, opacity=0.8)

#     # Optional: Add Quadrant Annotations (To help Executives read it instantly)
#     # These labels sit in the corners of the quadrants
#     # fig.add_annotation(x=95, y=95, text="SCALE 🚀", showarrow=False, xref="paper", yref="paper", font=dict(size=10, color="#10b981"))
#     # fig.add_annotation(x=5, y=5, text="CRISIS 🚨", showarrow=False, xref="paper", yref="paper", font=dict(size=10, color="#ef4444"))

#     # 4. Refine Tooltips
#     fig.update_traces(
#         hovertemplate="<br>".join([
#             "<b>%{hovertext}</b>",
#             "Earnings: $%{customdata[0]:,.2f}",
#             "Vol Change: %{customdata[1]:.1f}%",
#             "Eff Change: %{customdata[2]:.1f}%"
#         ])
#     )

#     fig.update_layout(
#         height=300,
#         template="plotly_white",
#         xaxis=dict(title="Volume Change %", zeroline=False, autorange=True), # Hide default zeroline to use our custom dashed one
#         yaxis=dict(title=f"{metric_type} Change %", zeroline=False, autorange=True),
#         showlegend=False,
#         margin=dict(l=10, r=10, t=30, b=10)
#     )

#     st.plotly_chart(fig, width='stretch')

    
#     # # ... (Keep your existing Quadrant Lines and Annotations logic here) ...

#     # # 4. Refine Tooltips
#     # fig.update_traces(
#     #     hovertemplate="<br>".join([
#     #         "<b>%{hovertext}</b>",
#     #         "Earnings: $%{customdata[0]:,.2f}",
#     #         "Vol Change: %{customdata[1]:.1f}%",
#     #         "Eff Change: %{customdata[2]:.1f}%"
#     #     ])
#     # )

#     # fig.update_layout(
#     #     height=300,
#     #     template="plotly_white",
#     #     xaxis=dict(title="Volume Change (vs 7D) %"),
#     #     yaxis=dict(title=f"{metric_type} Change (vs 7D) %"),
#     #     showlegend=False
#     # )

#     # st.plotly_chart(fig, width='stretch')




    

def render_deep_dive(hist_df, block_name):
    """Render EPC/EPI trend chart"""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=hist_df['Date'], y=hist_df['EPC'], name="EPC",
        line=dict(color='rgba(0, 123, 255, 0.8)', width=3),
        mode='lines+markers'
    ))
    fig.add_trace(go.Scatter(
        x=hist_df['Date'], y=hist_df['EPI'], name="EPI",
        line=dict(color='#ff8000', width=3), mode='lines+markers',
        yaxis='y2'
    ))
    
    fig.update_layout(
        title=f"💰 Price Trends: {block_name}",
        xaxis_title="Date",
        yaxis=dict(title="EPC ($)", side="left"),
        yaxis2=dict(title="EPI ($)", overlaying="y", side="right", showgrid=False, zeroline=False),
        hovermode="x unified", template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=350, margin=dict(l=50, r=50, t=60, b=50)
    )
    
    st.plotly_chart(fig, width='stretch')

    
def render_deep_dive_traffic(hist_df, block_name):
    """Render volume trend chart"""
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=hist_df['Date'], y=hist_df['Impressions'], name="Impressions",
        marker=dict(color='rgba(0, 123, 255, 0.7)', line=dict(width=0))
    ))
    fig.add_trace(go.Scatter(
        x=hist_df['Date'], y=hist_df['Clicks'], name="Clicks",
        line=dict(color='#ff8000', width=3), mode='lines+markers',
        yaxis='y2'
    ))
    
    fig.update_layout(
        title=f"📊 Volume Trends: {block_name}",
        xaxis_title="Date",
        yaxis=dict(title="Impressions"),
        yaxis2=dict(title="Clicks", overlaying="y", side="right", showgrid=False, zeroline=False),
        hovermode="x unified", template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=350, margin=dict(l=50, r=50, t=60, b=50)
    )
    
    st.plotly_chart(fig, width='stretch')



    
# --------------------------------------------------
# PARTNER DIALOG MODAL
# --------------------------------------------------



# @st.dialog("Partner Deep Dive", width="large")
# def open_partner_modal(selected_row, volume_df):
#     """Partner performance modal"""
#     partner = selected_row["Partner"]
#     selected_date = pd.to_datetime(selected_row["Date"]).date()
    
#     dfp = fetch_partner_block_history(partner, selected_date)
    
#     fig = go.Figure()
    
#     fig.add_trace(go.Bar(
#         y=dfp["Block Name"], x=dfp["Impressions"], orientation="h",
#         customdata=dfp[["Revenue", "Clicks"]].to_numpy(),
#         hovertemplate="<b>%{y}</b><br>Impressions: %{x:,}<br>Revenue: $%{customdata[0]:.2f}<br>Clicks: %{customdata[1]:,}<extra></extra>",
#         name="Impressions"
#     ))
    
#     fig.update_layout(
#         title=f"📈 Top Blocks by Impressions ({selected_date})",
#         template="plotly_white", yaxis_autorange="reversed",
#         height=500, margin=dict(l=10, r=10, t=60, b=20)
#     )
    
#     st.plotly_chart(fig, width='stretch')








@st.dialog("Partner Category Overview", width="large")
def open_partner_modal(selected_row):
    partner = selected_row["Partner"]
    
    selected_date = selected_row["Date"]

    df_cat = fetch_partner_category_snapshot(partner, selected_date)
    
    df_trend = fetch_partner_category_trend(partner)
    # df_trend = volume_df[volume_df["Partner"] == partner][["Date", "Live Categories"]]

    if df_cat.empty:
        st.warning("No category data available.")
        return

    # ---------------- KPI CALCULATIONS ----------------
    # live_categories = df_cat["Block Name"].nunique()
    # total_revenue = df_cat["Earnings"].sum()
    # total_impr = df_cat["Impressions"].sum()
    # total_clicks = df_cat["Clicks"].sum()

    # ---------------- KPI ROW ----------------
    # k1, k2, k3, k4 = st.columns(4)

    # k1.metric("🧩 Live Categories", f"{live_categories}")
    # k2.metric("💰 Revenue", f"${total_revenue:,.2f}")
    # k3.metric("👀 Impressions", f"{int(total_impr):,}")
    # k4.metric("🖱 Clicks", f"{int(total_clicks):,}")

    

    # ---------------- CHART ROW ----------------
    c1, c2 = st.columns(2)
    # c1 = st.columns(1)

    # with c1:
    #     fig_line = px.line(
    #         df_trend,
    #         x="Date",
    #         y="Live Categories",
    #         markers=True,
    #         title="📈 Live Category Count Trend (45 Days)"
    #     )
    #     fig_line.update_layout(height=350)#, yaxis=dict(showgrid=False))
    #     st.plotly_chart(fig_line, width='stretch')

    with c1:
        fig = go.Figure()
    
        # Line 1: Live Categories (left axis)
        fig.add_trace(go.Scatter(
            x=df_trend["Date"],
            y=df_trend["Live Categories"],
            line=dict(color='#007bff', width=2),
            #mode="lines+markers",
            name="Live Categories",
            yaxis="y1"
        ))
    
        # Line 2: Revenue (right axis)
        fig.add_trace(go.Scatter(
            x=df_trend["Date"],
            y=df_trend["Earnings"],
            line=dict(color='#ff8000', width=2.5),
            #mode="lines+markers",
            name="Earnings ($)",
            yaxis="y2"
        ))
    
        fig.update_layout(
            title="📈 Live Categories vs Earnings Trend (45 Days)",
            height=350,
            xaxis=dict(title="Date"),
            yaxis=dict(
                title="Live Categories",
                showgrid=False
            ),
            yaxis2=dict(
                title="Earnings",
                overlaying="y",
                side="right",
                showgrid=False,
                zeroline=False
            ),
            legend=dict(orientation="h", y=1.1)
        )
    
        st.plotly_chart(fig, width='stretch')


    # c2 = st.columns(1)
    
    with c2:
        fig_tree = px.treemap(
            df_cat,
            #path=["Block Name"],
            path=[px.Constant("All Categories"), "Block Name"],
            values="Earnings",
            custom_data=["Impressions", "Clicks"],
            title="🧩 Category Share by Earnings"
        )
        fig_tree.update_layout(height=350)

        fig_tree.update_traces(
            hovertemplate=
            "<b>%{label}</b><br><br>" +
            "💰 Earnings: %{value:$,.2f}<br>" +
            "👁 Impressions: %{customdata[0]:,}<br>" +
            "🖱 Clicks: %{customdata[1]:,}<br>" +
            "📊 Share: %{percentParent:.2%}<br>" +
            "<extra></extra>"
        )
        
        st.plotly_chart(fig_tree, width='stretch')

        #st.session_state["active_dialog"] = None




# --------------------------------------------------
# Category Modal
# --------------------------------------------------




@st.dialog("Category Deep Dive", width="large")
def open_category_modal(selected_row, end_date):
    
    target_block_id = selected_row["Block ID"]
    target_block_name = selected_row['Block Name']
    target_date = selected_row["Date"]
    
    st.markdown(f"""
    <h3>🔍 Block: {target_block_name}</strong></h3>
    <p><em>Date: {target_date} | Alert: {selected_row.get("Alerts", "N/A")}</em></p>
    """, unsafe_allow_html=True)
    
    # Fetch 45-day history
    hist_df = fetch_block_history(target_block_id)

    
    thirty_days_ago = end_date - timedelta(days=30)
    
    block_df = hist_df[hist_df['Date'] >= thirty_days_ago].copy().sort_values('Date')
    
    if block_df.empty:
        st.warning("No data available for this block.")
        return
    
    # Calculate 7D moving averages & CTR
    block_df['impr_7d_avg'] = block_df['Impressions'].rolling(7, min_periods=1).mean()
    block_df['epc_7d_avg'] = block_df['EPC'].rolling(7, min_periods=1).mean()


    
    # === VISUAL 1: Efficiency Scatter ===
    
    st.subheader("📊 Volume vs Efficiency")
    fig1 = go.Figure()
        
    # Today highlight
    latest_row = block_df.iloc[-1]
        
    fig1.add_trace(go.Scatter(
            x=[latest_row['Impressions']], y=[latest_row['EPC']],
            mode='markers+text', marker=dict(size=25, color='red', symbol='star'),
            textposition="top center", name='Today'
        ))
        
    # History
    fig1.add_trace(go.Scatter(
            x=block_df['Impressions'], y=block_df['EPC'],
            mode='markers', marker=dict(size=10, color='steelblue', opacity=0.7), name='History'
        ))
        
    # 7D avgs
    fig1.add_vline(x=block_df['impr_7d_avg'].iloc[-1], line_dash="dash", line_color="green")#, annotation_text="7D Impr Avg")
    fig1.add_hline(y=block_df['epc_7d_avg'].iloc[-1], line_dash="dash", line_color="orange")#, annotation_text="7D EPC Avg")


    avg_impr = block_df['impr_7d_avg'].iloc[-1]
    fig1.add_trace(go.Scatter(
        x=[avg_impr, avg_impr], y=[0, 1],
        mode='lines', line=dict(color='green', dash='dash', width=2),
        name='7D Avg Impressions', showlegend=True, hoverinfo='skip'
    ))
    
    avg_epc = block_df['epc_7d_avg'].iloc[-1]
    fig1.add_trace(go.Scatter(
        x=[0, 1], y=[avg_epc, avg_epc],
        mode='lines', line=dict(color='orange', dash='dash', width=2),
        name='7D Avg EPC', showlegend=True, hoverinfo='skip'
    ))

    
    fig1.update_layout(height=350, showlegend=True, hovermode='closest')
    fig1.update_xaxes(title_text="Impressions")
    fig1.update_yaxes(title_text="EPC ($)")
    st.plotly_chart(fig1, width='stretch')

    #st.session_state["active_dialog"] = None