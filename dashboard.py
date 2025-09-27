import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Load data từ CSV hoặc DB
df = pd.read_csv("data/fifa21_clean.csv")

st.title("FIFA21 Player Stats")

# Filter theo vị trí
position = st.selectbox("Chọn vị trí:", df["Best Position"].unique())
filtered = df[df["Best Position"] == position]

# Vẽ biểu đồ
fig, ax = plt.subplots()
filtered["OVA"].hist(ax=ax, bins=20)
st.pyplot(fig)
