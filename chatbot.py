import streamlit as st
import pandas as pd
from fpdf import FPDF
import io
import re
import google.generativeai as genai
from datetime import datetime
from queries.block_details import get_latest_event_date
from st_copy_to_clipboard import st_copy_to_clipboard


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

GEMINI_API_KEY = ""

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-3-flash-preview")



# --------------------------------------------------
# CUSTOM CSS FOR UI POLISH
# --------------------------------------------------
def apply_custom_styles():
    st.markdown("""
        <style>
        .stExpander {
            border: 1px solid #e0e0e0 !important;
            border-radius: 12px !important;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }
        .stButton button:hover {
            background-color: #007bff !important;
            color: white !important;
        }
        .chat-timestamp {
            font-size: 11px;
            color: #aaa;
            margin-top: 2px;
            margin-bottom: 6px;
        }
        </style>
    """, unsafe_allow_html=True)


# --------------------------------------------------
# DATA CONTEXT BUILDER
# --------------------------------------------------

def build_data_context(epc_df, epi_df, volume_df):

    context_parts = []

    # --- EPC Data ---
    if epc_df is not None and not epc_df.empty:
        latest_date = epc_df["Date"].max()
        
        latest_epc = epc_df[epc_df["Date"] == latest_date]

        red_epc = latest_epc[latest_epc["alert_bucket"] == "red"]
        green_epc = latest_epc[latest_epc["alert_bucket"] == "green"]

        context_parts.append(f"=== EPC TRACKER DATA (Latest Date: {latest_date}) ===")
        context_parts.append(f"Total blocks tracked: {latest_epc['Block ID'].nunique()}")
        context_parts.append(f"Red EPC alerts: {len(red_epc)}")
        context_parts.append(f"Green EPC alerts: {len(green_epc)}")

        if not red_epc.empty:
            context_parts.append("\nTop 10 RED EPC Alerts (by Earnings):")
            top_red = red_epc.nlargest(10, "Earnings")[
                ["Block ID", "Block Name", "Partner", "Earnings", "EPC", "7D Avg EPC", "Alerts", "Block's Daily Share"]
            ]
            context_parts.append(top_red.to_string(index=False))

        if not green_epc.empty:
            context_parts.append("\nTop 10 GREEN EPC Alerts (by Earnings):")
            top_green = green_epc.nlargest(10, "Earnings")[
                ["Block ID", "Block Name", "Partner", "Earnings", "EPC", "7D Avg EPC", "Alerts", "Block's Daily Share"]
            ]
            context_parts.append(top_green.to_string(index=False))

        # Full alert data for all dates (for historical questions)
        context_parts.append(f"\nFull EPC alert data across all dates (alerts only, top 50 by Earnings):")
        all_alerts_epc = epc_df[epc_df["alert_bucket"] != "no impact"].nlargest(50, "Earnings")[
            ["Date", "Block ID", "Block Name", "Partner", "Earnings", "EPC", "7D Avg EPC", "alert_bucket", "Alerts"]
        ]
        context_parts.append(all_alerts_epc.to_string(index=False))

    # --- EPI Data ---
    if epi_df is not None and not epi_df.empty:
        latest_date = epi_df["Date"].max()
        latest_epi = epi_df[epi_df["Date"] == latest_date]

        red_epi = latest_epi[latest_epi["alert_bucket"] == "red"]
        green_epi = latest_epi[latest_epi["alert_bucket"] == "green"]

        context_parts.append(f"\n=== EPI TRACKER DATA (Latest Date: {latest_date}) ===")
        context_parts.append(f"Total blocks tracked: {latest_epi['Block ID'].nunique()}")
        context_parts.append(f"Red EPI alerts: {len(red_epi)}")
        context_parts.append(f"Green EPI alerts: {len(green_epi)}")

        if not red_epi.empty:
            context_parts.append("\nTop 10 RED EPI Alerts (by Earnings):")
            top_red_epi = red_epi.nlargest(10, "Earnings")[
                ["Block ID", "Block Name", "Partner", "Earnings", "EPI", "7D Avg EPI", "Alerts", "Block's Daily Share"]
            ]
            context_parts.append(top_red_epi.to_string(index=False))

        if not green_epi.empty:
            context_parts.append("\nTop 10 GREEN EPI Alerts (by Earnings):")
            top_green_epi = green_epi.nlargest(10, "Earnings")[
                ["Block ID", "Block Name", "Partner", "Earnings", "EPI", "7D Avg EPI", "Alerts", "Block's Daily Share"]
            ]
            context_parts.append(top_green_epi.to_string(index=False))

    # --- Volume Spike Data ---
    if volume_df is not None and not volume_df.empty:
        latest_date = volume_df["Date"].max()
        latest_vol = volume_df[volume_df["Date"] == latest_date]

        context_parts.append(f"\n=== PARTNER VOLUME SPIKE TRACKER (Latest Date: {latest_date}) ===")
        context_parts.append(f"Partners tracked: {latest_vol['Partner'].nunique()}")
        context_parts.append(latest_vol[
            ["Partner", "Earnings", "Impressions", "Clicks", "CTR",
             "Clicks vs 7D", "Impr vs 7D", "Rev vs 7D", "Partner's Daily Share", "Alerts"]
        ].to_string(index=False))

    return "\n".join(context_parts)


# --------------------------------------------------
# SYSTEM PROMPT
# --------------------------------------------------

def system_prompt(data_context):
    return f"""
            You are a KPI Analytics Assistant named "DkamP007" embedded inside a performance monitoring dashboard called "KPI Alert Console".
            Your job is to help users — including executives and analysts — understand the data currently loaded in the dashboard.
            
            You have access to the following live dashboard data:
            
            {data_context}
            
            GUIDELINES:
            - You can ONLY answer using the data above.
            - If asked about a specific block, partner, or date, look it up in the data and give a precise answer.
            - When explaining alerts, explain what they mean in plain English (e.g. "EPC dropped more than 20% below its 7-day average").
            - Always mention the date the data refers to when giving specific numbers.
            - If the user asks something you cannot answer from the data (e.g. data not in the loaded window), say so clearly and suggest they adjust the dashboard filters.
            - Do NOT make up numbers. Only use data provided above.
            - Do NOT guess or invent data.
            - Keep responses brief and to the point. Use bullet points for lists.
            - You are read-only — you cannot change any data or settings in the dashboard.
            - Today's context: {get_latest_event_date()}
            """




# --------------------------------------------------
# PDF GENERATOR
# --------------------------------------------------

def strip_markdown(text):
    """
    Converts markdown to clean plain text suitable for PDF rendering.
    Handles bold, italic, bullet points, headers, inline code,
    and sanitizes special unicode characters unsupported by Helvetica.
    """
    # Headers → plain text (remove # symbols)
    text = re.sub(r"#{1,6}\s*", "", text)
    # Bold **text** or __text__
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"__(.*?)__", r"\1", text)
    # Italic *text* or _text_
    text = re.sub(r"\*(.*?)\*", r"\1", text)
    text = re.sub(r"_(.*?)_", r"\1", text)
    # Inline code `text`
    text = re.sub(r"`(.*?)`", r"\1", text)
    # Bullet points: * item or - item → • item
    text = re.sub(r"^\s*[\*\-]\s+", "  - ", text, flags=re.MULTILINE)

    # Sanitize special unicode chars that Helvetica cannot render
    replacements = {
        "\u2014": "-",   # em dash —
        "\u2013": "-",   # en dash –
        "\u2018": "'",   # left single quote '
        "\u2019": "'",   # right single quote '
        "\u201C": '"',   # left double quote "
        "\u201D": '"',   # right double quote "
        "\u2026": "...", # ellipsis …
        "\u2022": "-",   # bullet •
        "\u00A0": " ",   # non-breaking space
    }
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)

    # Final catch-all: strip anything outside latin-1 range
    text = re.sub(r"[^\x00-\xFF]", "", text)
    return text.strip()


def create_pdf(history):
    pdf = FPDF()
    pdf.set_margins(15, 15, 15)
    pdf.add_page()

    # --------------------------------------------------
    # Header block
    # --------------------------------------------------
    # Top accent bar
    pdf.set_fill_color(30, 30, 30)
    pdf.rect(0, 0, 210, 18, "F")

    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(255, 255, 255)
    pdf.set_xy(0, 4)
    pdf.cell(0, 10, "DkamP007 - KPI Analytics Chat Report", ln=True, align='C')

    pdf.set_text_color(33, 33, 33)
    pdf.ln(6)

    # Generated on + divider
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(0, 6, f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}   |   KPI Alert Console", ln=True, align="C")
    pdf.ln(3)
    pdf.set_draw_color(220, 220, 220)
    pdf.line(15, pdf.get_y(), 195, pdf.get_y())
    pdf.ln(6)

    # --------------------------------------------------
    # Messages
    # --------------------------------------------------
    for msg in history:
        is_user = msg["role"] == "user"
        timestamp = msg.get("timestamp", "")

        # Clean content — strip markdown for clean PDF text
        content = strip_markdown(msg["content"])

        if is_user:
            # ------ USER message ------
            # Role label with timestamp
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(255, 255, 255)
            pdf.set_fill_color(59, 130, 246)  # blue
            label = f"  You"
            if timestamp:
                label += f"   {timestamp}"
            pdf.cell(0, 7, label, ln=True, fill=True)

            # Message content
            pdf.set_font("Helvetica", "", 10)
            pdf.set_text_color(33, 33, 33)
            pdf.set_fill_color(239, 246, 255)  # light blue bg
            pdf.multi_cell(0, 6, f"  {content}", fill=True)

        else:
            # ------ ASSISTANT message ------
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(255, 255, 255)
            pdf.set_fill_color(30, 30, 30)  # dark
            label = f"  DkamP007"
            if timestamp:
                label += f"   {timestamp}"
            pdf.cell(0, 7, label, ln=True, fill=True)

            # Message content
            pdf.set_font("Helvetica", "", 10)
            pdf.set_text_color(33, 33, 33)
            pdf.set_fill_color(247, 247, 247)  # light grey bg
            pdf.multi_cell(0, 6, f"  {content}", fill=True)

        pdf.ln(4)

    # --------------------------------------------------
    # Footer
    # --------------------------------------------------
    pdf.set_y(-15)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(180, 180, 180)
    pdf.cell(0, 10, "KPI Alert Console  |  Confidential", align="C")

    pdf_bytes = pdf.output(dest="S")
    if isinstance(pdf_bytes, bytearray):
        pdf_bytes = bytes(pdf_bytes)
    return pdf_bytes



# --------------------------------------------------
# CHATBOT UI RENDERER
# --------------------------------------------------


def render_chatbot():
    apply_custom_styles()

    with st.expander("DkamP007 — KPI Assistant", expanded=True):

        # Guard: tracker must have run first
        if not st.session_state.get("tracker_ran"):
            st.info("Please run the tracker first to load data into the Assistant.")
            return

        # --------------------------------------------------
        # Init chat history with welcome message on first load
        # --------------------------------------------------
        if "chat_history" not in st.session_state:
            latest_date = get_latest_event_date()
            st.session_state.chat_history = [
                {
                    "role": "assistant",
                    "content": (
                        f"👋 Hi! I'm **DkamP007**, your KPI Assistant. I have the latest dashboard data loaded up to **{latest_date}**.\n\n"
                        "You can ask me things like:\n"
                        "- *Which blocks have red EPC alerts today?*\n"
                        "- *What is the top earning block for partner BRN?*\n"
                        "- *Explain the alert for block 6472*\n"
                        "- *Show me partners with volume spikes today*"
                    ),
                    "timestamp": datetime.now().strftime("%H:%M")
                }
            ]
        # --------------------------------------------------
        # Quick action buttons
        # --------------------------------------------------
        st.markdown(
            "<p style='font-size:13px; color:#666; margin-bottom:5px;'>Quick Queries for DkamP007:</p>",
            unsafe_allow_html=True
        )

        cols = st.columns(5)

        with cols[0]:
            if st.button("🚩 Today's Red Alerts", use_container_width=True):
                st.session_state["suggested_query"] = "Summarize all Red EPC & EPI alerts for today in a table format."
                st.rerun()

        with cols[1]:
            if st.button("💰 Top 5 Blocks Today", use_container_width=True):
                st.session_state["suggested_query"] = "Which 5 blocks have the highest earnings today? Show in a table with their alert status."
                st.rerun()

        with cols[2]:
            if st.button("📊 Partner Spikes", use_container_width=True):
                st.session_state["suggested_query"] = "Show me all partners with significant volume spikes today and explain each alert."
                st.rerun()

        with cols[3]:
            # PDF export — disabled if no real conversation yet (only welcome msg)
            has_conversation = len(st.session_state.chat_history) > 1
            if has_conversation:
                pdf_data = create_pdf(st.session_state.chat_history)
                st.download_button(
                    "📄 Export PDF",
                    data=pdf_data,
                    file_name=f"KPI_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                    mime="application/pdf",
                    use_container_width=True
                )
            else:
                st.button(
                    "📄 Export PDF",
                    disabled=True,
                    use_container_width=True,
                    help="Start a conversation first to enable PDF export"
                )

        with cols[4]:
            if st.button("🗑️ Clear Chat", use_container_width=True):
                st.session_state.chat_history = []
                st.session_state.pop("suggested_query", None)
                st.rerun()

        # --------------------------------------------------
        # Session message count indicator
        # --------------------------------------------------
        msg_count = len(st.session_state.chat_history)
        st.caption(f"💬 {msg_count} message{'s' if msg_count != 1 else ''} in this session")

        # --------------------------------------------------
        # Chat display container (scrollable)
        # --------------------------------------------------
        chat_container = st.container(height=400)

        with chat_container:
            for msg in st.session_state.chat_history:
                display_role = "assistant" if msg["role"] == "assistant" else "user"
                with st.chat_message(display_role):
                    st.markdown(msg["content"])
                    if msg.get("timestamp"):
                        st.markdown(
                            f"<div class='chat-timestamp'>{msg['timestamp']}</div>",
                            unsafe_allow_html=True
                        )
                    # Copy button only for assistant messages
                    if msg["role"] == "assistant":
                        st_copy_to_clipboard(msg["content"])

        # --------------------------------------------------
        # Handle input — typed OR suggested query from button
        # --------------------------------------------------
        user_input = st.chat_input("Ask DkamP007 about today's performance...")

        # Fix: pop suggested_query from session state to fire correctly
        # without conflicting with st.chat_input
        if not user_input and st.session_state.get("suggested_query"):
            user_input = st.session_state.pop("suggested_query")

        if not user_input:
            return

        # --------------------------------------------------
        # Append user message to history
        # --------------------------------------------------
        timestamp_now = datetime.now().strftime("%H:%M")
        st.session_state.chat_history.append({
            "role": "user",
            "content": user_input,
            "timestamp": timestamp_now
        })

        # --------------------------------------------------
        # Build data context + system prompt fresh on every turn
        # This ensures Gemini always has full dashboard context
        # regardless of turn number in the conversation
        # --------------------------------------------------
        data_context = build_data_context(
            st.session_state.get("epc_df"),
            st.session_state.get("epi_df"),
            st.session_state.get("volume_df")
        )
        prompt = system_prompt(data_context)
        full_prompt = f"{prompt}\n\nUser: {user_input}"

        # --------------------------------------------------
        # Generate streaming response from Gemini
        # --------------------------------------------------
        with chat_container:
            with st.chat_message("assistant"):
                placeholder = st.empty()
                full_response = ""
                response_timestamp = datetime.now().strftime("%H:%M")

                try:
                    with st.spinner("DkamP007 is thinking..."):
                        response_stream = model.generate_content(full_prompt, stream=True)

                    for chunk in response_stream:
                        if chunk.text:
                            full_response += chunk.text
                            placeholder.markdown(full_response + "▌")

                    # Final clean render without cursor
                    placeholder.markdown(full_response)

                    # Timestamp
                    st.markdown(
                        f"<div class='chat-timestamp'>{response_timestamp}</div>",
                        unsafe_allow_html=True
                    )

                    # Copy button
                    st_copy_to_clipboard(full_response)

                except Exception as e:
                    full_response = f"⚠️ Analytics Error: {str(e)}"
                    placeholder.error(full_response)

        # --------------------------------------------------
        # Save assistant response to history
        # --------------------------------------------------
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": full_response,
            "timestamp": response_timestamp
        })
