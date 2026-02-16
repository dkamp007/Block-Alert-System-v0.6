import streamlit as st
import pandas as pd
from datetime import timedelta
from queries.block_details import (
    fetch_complete_mapping, get_latest_event_date, fetch_block_history, 
    apply_display_days, render_sidebar, render_kpis, render_alert_table, 
    render_alert_table_volume, render_deep_dive, render_deep_dive_traffic, 
    open_partner_modal, apply_user_filters, open_category_modal
)
from queries.epc_tracker import fetch_epc_tracker
from queries.epi_tracker import fetch_epi_tracker
from queries.spike_tracker import fetch_volume_spike_tracker, fetch_category_spike_tracker


# --------------------------------------------------
# PAGE CONFIG & STYLES
# --------------------------------------------------
st.set_page_config("Block System Alert Console v0.5", layout="wide", initial_sidebar_state='expanded')

st.markdown("""
<style>
    /*html, body, [class*="st-"]  {*/
    /*    font-size: 14px;*/


    div[data-testid='stSidebarHeader'] {
        display: flex;
        -webkit-box-pack: justify;
        justify-content: space-between;
        -webkit-box-align: center;
        align-items: center;
        margin-bottom: 0rem;
        height: 2.5rem;
    }
    
    div[data-testid='stSidebarContent'] div[data-testid='stMarkdownContainer'] {
        font-size: 15px;
    }


    [data-testid='stMainBlockContainer'] {
        padding-top: 1.5rem;
        padding-left: 4rem;
        padding-right: 4rem;
        padding-bottom: 12rem;
    }
    

    [data-testid="stMetric"] {
        background-color: transparent;
        border: 1.5px solid #e0e0e0;
        border-radius: 13px;
        border-color: transparent;
        box-shadow: 0 8px 25px rgba(31, 38, 135, 0.5);
        padding: 15px;
        width: 100%;
        /*min-width: 240px;*/
        flex-grow: 1;
        height: 80%;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        animation: fadeIn 0.5s ease-in-out;
    }
    [data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 24px rgba(0,0,0,0.12);
        border-color: rgba(59, 130, 246, 0.5);
    }
    
    [data-testid="stMetricLabel"], [data-testid="stMetricValue"] {
        text-align: center !important;
        display: block;
    }
    
    [data-testid="stMetric"] > div > label > div > div {
        font-size: 13.5px;
        font-weight: 500;
        text-align: center; !important;
        display: block; !important;
        margin-bottom: 4px;
        /*grid-template-columns: auto 0fr;*/
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 22px !important;
        font-weight: 600;
        letter-spacing: -0.02em;
        text-align: center;
        /*margin-bottom: 20px;*/
        padding-bottom: 1.25rem;
    }
    
    
    .stTabs > div > div > div[data-baseweb="tab-list"] > button[data-baseweb="tab"] > div[data-testid=stMarkdownContainer] {
        font-size: 12.5px; !important;  
        font-weight: 500; !important;
        /*color: #6b7280;*/
    }
    .stTabs [aria-selected="true"] {
        border-bottom: 1px solid #3b82f6;
    }

    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.markdown("""
<div class="dashboard-header">
    <h1>📊 KPI Alert Console v0.5</h1>
    
</div>
<style>
    .dashboard-header {
        /*margin-bottom: 2rem;*/
        padding-top: 0rem;
        padding-left: 0rem;
        padding-right: 0rem;
        padding-bottom: 1rem;
    }
    .dashboard-header h1 {
        font-size: clamp(1rem, 4vw, 2rem);
        line-height: 1.2;
        margin: 0 0 0.5rem 0;
        font-weight: 700;
        text-align: center;
        /*color: #1e293b;*/
    }
    .dashboard-header p {
        font-size: clamp(0.5rem, 2.5vw, 1rem);
        line-height: 1.5;
        margin: 0;
        /*color: #64748b;*/
        font-weight: 600;
    }
    
    /* Responsive breakpoints */
    @media (max-width: 768px) {
        .dashboard-header h1 { font-size: 1.75rem !important; }
        .dashboard-header p { font-size: 0.9rem !important; }
    }
</style>
""", unsafe_allow_html=True)

#st.divider()

# --------------------------------------------------
# DATA SETUP
# --------------------------------------------------

mapping_df = fetch_complete_mapping()
partners, block_ids, block_names, run_btn, display_days, custom_range = render_sidebar(mapping_df, get_latest_event_date())

# --------------------------------------------------
# DATA FETCH & VALIDATION
# --------------------------------------------------

if "tracker_ran" not in st.session_state:
    st.session_state["tracker_ran"] = False

if run_btn:
    with st.spinner("Running trackers..."):        
        st.session_state["epc_df"] = fetch_epc_tracker(
            partners=tuple(sorted(partners or [])), 
            block_names=tuple(sorted(block_names or [])), 
            block_ids=tuple(sorted(block_ids or []))
        )
        st.session_state["epi_df"] = fetch_epi_tracker(
            partners=tuple(sorted(partners or [])), 
            block_names=tuple(sorted(block_names or [])), 
            block_ids=tuple(sorted(block_ids or []))
        )
        
        st.session_state["volume_df"] = fetch_volume_spike_tracker(partners or [])

        st.session_state['category_df'] = fetch_category_spike_tracker(
            block_names=tuple(sorted(block_names or [])), 
            block_ids=tuple(sorted(block_ids or []))
        )
        
        st.session_state["tracker_ran"] = True


required_keys = {"epc_df", "epi_df", "volume_df", 'category_df'} #"combined_df"

if not required_keys.issubset(st.session_state) or not st.session_state["tracker_ran"]:
    st.info("Click **Run Tracker** to view alerts.")
    st.stop()

# if "active_dialog" not in st.session_state:
#     st.session_state["active_dialog"] = None

# Extracting core data
epc_df = st.session_state["epc_df"]
epi_df = st.session_state["epi_df"]
#combined_df = st.session_state["combined_df"]
volume_df = st.session_state["volume_df"]
category_df = st.session_state['category_df']



# --------------------------------------------------
# KPI CONTROLS
# --------------------------------------------------

col1, col2, col3 = st.columns(3)


st.markdown("""
        <style>
        
        div[data-testid="stRadio"] > label > div:first-child {
            font-size: 14px !important;
            font-weight: 600 !important; }
        
        div[data-testid="stRadio"] div[role="radiogroup"] label div {
            font-size: 14px !important;
            font-weight: 500 !important;
        }
        
        </style>
        """, 
        unsafe_allow_html=True)

with col1:
       
    view = st.radio(
        "**KPI Source**", 
        ["EPC", "EPI"],
        index=0, #.index(st.session_state.kpi_view),
        horizontal=True, 
        key="kpi_source_radio"
    )
    #st.session_state.kpi_view = view

with col3:
    
    alert_view = st.radio(
        "**Alert Filters**", 
        ["ALL", "RED", "GREEN"], 
        index=1,
        format_func=lambda x: {"ALL": "📊 All", "RED": "🔴 Alerts", "GREEN": "🟢 Alerts"}[x], 
        horizontal=True)


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def apply_alert_filter(df):
    if alert_view == "RED":
        filtered = df[(df["alert_bucket"] == "red") & (df["alert_bucket"] != "no impact") & (df['alert_bucket'] != 'green')]
    elif alert_view == "GREEN":
        filtered = df[(df["alert_bucket"] == "green") & (df["alert_bucket"] != "no impact") & (df['alert_bucket'] != 'red')]
    else:
        filtered = df
    
    return filtered


def prepare_for_display(df):
    df_display = apply_display_days(df, date_col="Date", days=display_days, date_range=custom_range)
    df_filtered = apply_alert_filter(df_display)
    return df_filtered


# --------------------------------------------------
# KPI RENDERING
# --------------------------------------------------

kpi_df_display = apply_display_days(epc_df if view == "EPC" else epi_df, date_col="Date", days=display_days, date_range=custom_range)
kpi_final = apply_user_filters(kpi_df_display, partners, block_names, block_ids)
kpi_filtered = apply_alert_filter(kpi_final)

render_kpis(kpi_final, kpi_filtered, alert_view)


# --------------------------------------------------
# USER FILTERING
# --------------------------------------------------

epc_filtered = apply_user_filters(epc_df, partners, block_names, block_ids)
epi_filtered = apply_user_filters(epi_df, partners, block_names, block_ids)
#combined_df_filtered = apply_user_filters(combined_df, partners, block_names, block_ids)
volume_filtered = apply_user_filters(volume_df, partners, None, None)
cat_filtered = apply_user_filters(category_df, None, block_names, block_ids)

# --------------------------------------------------
# MAIN TABS
# --------------------------------------------------

if "active_tab" not in st.session_state:
    st.session_state["active_tab"] = None


tab1, tab2, tab3, tab4 = st.tabs(["**📋 EPC Alerts**", "**📋 EPI Alerts**", "**📈 Partner Volume Spike Tracker**", "**📈 Category Volume Spike Tracker**"])

with tab1:
    filtered_epc = prepare_for_display(epc_filtered)
    if filtered_epc.empty:
        st.info(f"No {alert_view} EPC alerts for selected window.")
    else:
        render_alert_table(filtered_epc, {
            #"Block's 45D Share": st.column_config.ProgressColumn("Block's 60D Share", format="%.2f%%", min_value=0, max_value=10),
            'Partner Share': st.column_config.ProgressColumn("Partner Share", format="%.2f%%", min_value=0, max_value=100),
            'EPC': st.column_config.NumberColumn("EPC", format="$%.4f"),#format='dollar'),#,
            '7D Avg EPC': st.column_config.NumberColumn("7D Avg EPC", format="$%.4f"),# format='dollar'),# format="$%.4f"),
            'Earnings': st.column_config.NumberColumn("Earnings", format='dollar'),#format="$%.2f"),
            '7D Avg Earnings': st.column_config.NumberColumn("7D Avg Earnings", format='dollar'),# format="$%.2f"),
            'Date': st.column_config.DateColumn('Date', format='YYYY-MM-DD', pinned=True),
            'Block ID': st.column_config.NumberColumn('Block ID', pinned=True),
            "Block's Daily Share": st.column_config.ProgressColumn("Block's Daily Share", format="%.2f%%", min_value=0.0001, max_value=20),
            'alert_bucket': None,
            '30D Avg EPC': st.column_config.NumberColumn("30D Avg EPC", format="$%.4f"),
            '30D Avg Earnings': st.column_config.NumberColumn("30D Avg Earnings", format="$%.2f")# format="$%.2f"),
        })

with tab2:
    filtered_epi = prepare_for_display(epi_filtered)
    if filtered_epi.empty:
        st.info(f"No {alert_view} EPI alerts for selected window.")
    else:
        render_alert_table(filtered_epi, {
            #"Block's 60D Share": st.column_config.ProgressColumn("Block's 60D Share", format="%.2f%%", min_value=0, max_value=10),
            'Partner Share': st.column_config.ProgressColumn("Partner Share", format="%.2f%%", min_value=0, max_value=100),
            'EPI': st.column_config.NumberColumn("EPI", format="$%.4f"),# format='dollar'),# format="$%.4f"),
            '7D Avg EPI': st.column_config.NumberColumn("7D Avg EPI", format="$%.4f"),# format='dollar'),# format="$%.4f"),
            'Earnings': st.column_config.NumberColumn("Earnings", format='dollar'),# format="$%.2f"),
            '7D Avg Earnings': st.column_config.NumberColumn("7D Avg Earnings", format='dollar'),# format="$%.2f"),
            'Date': st.column_config.DateColumn('Date', format='YYYY-MM-DD', pinned=True),
            'Block ID': st.column_config.NumberColumn('Block ID', pinned=True),
            "Block's Daily Share": st.column_config.ProgressColumn("Block's Daily Share", format="%.2f%%", min_value=0.0001, max_value=20),
            'alert_bucket': None,
            '30D Avg EPI': st.column_config.NumberColumn("30D Avg EPI", format="$%.4f"),
            '30D Avg Earnings': st.column_config.NumberColumn("30D Avg Earnings", format="$%.2f")
        })

with tab3:
    #volume_filtered = st.session_state.get("volume_df")

    if st.session_state.get("active_tab") != "partner":
        st.session_state["active_tab"] = "partner"
        st.session_state.pop("last_partner_selection", None)
    
    if volume_filtered is None or volume_filtered.empty:
        st.info("No volume spike data available.")
    else:
        final_volume_df = apply_display_days(volume_filtered, "Date", display_days, custom_range)
        
        event = render_alert_table_volume(final_volume_df, {
            'Impressions': st.column_config.NumberColumn('Impressions', format="%d"),
            'Clicks': st.column_config.NumberColumn('Clicks', format="%d"),
            'CTR': st.column_config.NumberColumn('CTR', format="%.2f%%"),
            'Earnings': st.column_config.NumberColumn("Earnings", format='dollar'),# format="$%.2f"),
            'Clicks vs 7D': st.column_config.ProgressColumn("Clicks vs 7D", min_value=-100, max_value=800, format="%.2f%%"),
            'Impr vs 7D': st.column_config.ProgressColumn("Impr vs 7D", min_value=-100, max_value=800, format="%.2f%%"),
            'Rev vs 7D': st.column_config.ProgressColumn("Rev vs 7D", min_value=-100, max_value=800, format="%.2f%%"),
            "Partner's Daily Share": st.column_config.ProgressColumn("Partner's Daily Share", min_value=0.0001, max_value=40, format="%.2f%%"),
            'Date': st.column_config.DateColumn('Date', format='YYYY-MM-DD', pinned=True),
            'Partner': st.column_config.TextColumn('Partner', pinned=True)
        }, 
        selectable=True, 
        selection_mode="single-row", 
        key="volume_table_select")

        # if event and event.selection and event.selection.rows:
        #     selected_row = final_volume_df.iloc[event.selection.rows[0]]
        #     # open_partner_modal(selected_row, final_volume_df)
        #     open_partner_modal(selected_row)
        #     st.write("Selected:", selected_row["Partner"], selected_row["Date"])

        if event and event.selection and event.selection.rows:
            selected_row = final_volume_df.iloc[event.selection.rows[0]]
        
            key = (selected_row["Partner"], selected_row["Date"])
        
            if st.session_state.get("last_partner_selection") != key:
                st.session_state["last_partner_selection"] = key
                open_partner_modal(selected_row)
        
            st.write("Selected:", selected_row["Partner"], selected_row["Date"])

with tab4:
    if st.session_state.get("active_tab") != "category":
        st.session_state["active_tab"] = "category"
        st.session_state.pop("last_category_selection", None)
    
    if cat_filtered is None or cat_filtered.empty:
        st.info('No block spike data available.')
    else:
        final_cat_df = apply_display_days(cat_filtered, 'Date', display_days, custom_range)
        cat_df = render_alert_table_volume(final_cat_df, {
            "Block's 45D Share": st.column_config.ProgressColumn("Block's 45D Share", format="%.2f%%", min_value=0.0, max_value=10),
            "Block's Daily Share": st.column_config.ProgressColumn("Block's Daily Share", format="%.2f%%", min_value=0.0001, max_value=20),
            'Impressions': st.column_config.NumberColumn('Impressions', format="%d"),
            'Clicks': st.column_config.NumberColumn('Clicks', format="%d"),
            'CTR': st.column_config.NumberColumn('CTR', format="%.2f%%"),
            'Date': st.column_config.DateColumn('Date', format='YYYY-MM-DD', pinned=True),
            'Block Name': st.column_config.TextColumn('Block Name'),
            'Block ID': st.column_config.NumberColumn('Block ID', pinned=True),
            'Clicks vs 7D': st.column_config.ProgressColumn("Clicks vs 7D", min_value=-100, max_value=800, format="%.2f%%"),
            'Impr vs 7D': st.column_config.ProgressColumn("Impr vs 7D", min_value=-100, max_value=800, format="%.2f%%"),
            'Rev vs 7D': st.column_config.ProgressColumn("Rev vs 7D", min_value=-100, max_value=800, format="%.2f%%"),
            'Earnings': st.column_config.NumberColumn("Earnings", format='dollar')# format="$%.2f")
        }, 
        selectable=True, 
        selection_mode="single-row", 
        key="category_volume_table_select")

        # if cat_df and cat_df.selection and cat_df.selection.rows:
        #     selected_row = final_cat_df.iloc[cat_df.selection.rows[0]]
        #     end_date = get_latest_event_date()
        #     open_category_modal(selected_row, end_date)
        #     st.write("Selected:", selected_row["Block Name"], selected_row["Date"])

        if cat_df and cat_df.selection and cat_df.selection.rows:
            selected_row = final_cat_df.iloc[cat_df.selection.rows[0]]
        
            end_date = get_latest_event_date()
            key = (selected_row["Block ID"], selected_row["Date"])
        
            if st.session_state.get("last_category_selection") != key:
                st.session_state["last_category_selection"] = key
                open_category_modal(selected_row, end_date)
        
            st.write("Selected:", selected_row["Block Name"], selected_row["Date"])

# --------------------------------------------------
# DEEP DIVE SECTION
# --------------------------------------------------
#st.divider()

epc_flagged = epc_df[epc_df['Alerts'] != 'Within Thresholds'][['Block Name', 'Block ID', 'Partner']].drop_duplicates()
epi_flagged = epi_df[epi_df['Alerts'] != 'Within Thresholds'][['Block Name', 'Block ID', 'Partner']].drop_duplicates()

epc_alerts_filtered = apply_user_filters(epc_flagged, partners, block_names, block_ids)
epi_alerts_filtered = apply_user_filters(epi_flagged, partners, block_names, block_ids)

all_flagged_blocks = pd.concat([epc_alerts_filtered, epi_alerts_filtered]).drop_duplicates().sort_values("Block Name")

if not all_flagged_blocks.empty:
    flagged_names = all_flagged_blocks["Block Name"].unique()
    target_block_name = st.selectbox(
        "**Select flagged block for deep dive:**", 
        options=flagged_names, 
        index=None, 
        placeholder="Choose block to see trends"
    )
    
    if target_block_name:
        target_id = all_flagged_blocks[all_flagged_blocks["Block Name"] == target_block_name]['Block ID'].iloc[0]
        with st.spinner(f"Fetching 45-day history for {target_block_name}..."):
            hist_df = fetch_block_history(target_id)
            col1, col2 = st.columns(2)
            with col1:
                render_deep_dive(hist_df, target_block_name)
            with col2:
                render_deep_dive_traffic(hist_df, target_block_name)