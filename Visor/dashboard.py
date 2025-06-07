import streamlit as st
import pandas as pd
import os

st.title("Project Dashboard")

DATA_DIR = "./dashboard_data"

for filename in os.listdir(DATA_DIR):
    if filename.endswith(".csv"):
        st.subheader(f"{filename}")
        df = pd.read_csv(os.path.join(DATA_DIR, filename))
        st.dataframe(df)

        # Optional: sample chart
        if "rating" in df.columns:
            st.line_chart(df["rating"])
