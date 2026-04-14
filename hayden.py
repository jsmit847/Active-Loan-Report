
# CLEAN VERSION: NO SHAREPOINT + FIXED SIDE PANEL + NO SIDEBAR

import streamlit as st
from datetime import date
import pandas as pd

st.set_page_config(layout="wide")

# ---- HARD HIDE STREAMLIT SIDEBAR + PLUS ----
st.markdown("""
<style>
[data-testid="stSidebar"] {display: none !important;}
[data-testid="collapsedControl"] {display: none !important;}
</style>
""", unsafe_allow_html=True)

# ---- SAMPLE DATA (replace with your real pipeline) ----
df = pd.DataFrame({
    "Deal": ["Deal A","Deal B","Deal C"],
    "Asset Manager": ["Chris","Chris","Alex"],
    "UPB": [1000000,2000000,1500000]
})

# ---- LEFT CONTROL PANEL ----
left, right = st.columns([1,4])

with left:
    st.markdown("## Controls")

    order = st.selectbox(
        "Presentation order",
        ["Workbook file order","Asset Manager order"]
    )

    ams = ["All"] + sorted(df["Asset Manager"].unique())
    am_filter = st.selectbox("Show deals for", ams)

# ---- APPLY LOGIC ----
filtered = df.copy()

if am_filter != "All":
    filtered = filtered[filtered["Asset Manager"] == am_filter]

if order == "Asset Manager order":
    filtered = filtered.sort_values("Asset Manager")

# ---- MAIN VIEW ----
with right:
    st.markdown("## Deals")

    for _, row in filtered.iterrows():
        st.markdown(f"""
### {row['Deal']}
**AM:** {row['Asset Manager']}  
**UPB:** ${row['UPB']:,}
""")
