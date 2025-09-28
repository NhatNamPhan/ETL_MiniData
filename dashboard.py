import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv("data/fifa21_clean.csv")

st.title("FIFA21 Player Stats Dashboard")

# --- Bộ lọc ---
# Filter theo vị trí
# position = st.selectbox("Chọn vị trí:", df["Best Position"].unique())
# filtered = df[df["Best Position"] == position]

# Filter theo câu lạc bộ
club = st.selectbox("Chọn câu lạc bộ:", df["Club"].unique())
filtered = df[df["Club"] == club]

# --- Biểu đồ cơ bản ---
# Histogram OVA
fig, ax = plt.subplots()
filtered["OVA"].hist(ax=ax, bins=20)
ax.set_title("Phân bố OVA")
st.pyplot(fig)

# Histogram Best Position
fig, ax = plt.subplots()
filtered["Best Position"].hist(ax=ax, bins=20)
ax.set_title("Phân bố vị trí cầu thủ")
st.pyplot(fig)

# --- Biểu đồ mở rộng ---
# Phân bố độ tuổi cầu thủ
fig, ax = plt.subplots()
df["Age"].hist(ax=ax, bins=20)
ax.set_title("Phân bố độ tuổi cầu thủ")
st.pyplot(fig)

# Top 10 quốc gia có nhiều cầu thủ nhất
fig, ax = plt.subplots()
df["Nationality"].value_counts().head(10).plot(kind="bar", ax=ax)
ax.set_title("Top 10 quốc gia có nhiều cầu thủ nhất")
st.pyplot(fig)

# Lương trung bình theo vị trí (Top 10)
fig, ax = plt.subplots()
df.groupby("Best Position")["Wage in Euro"].mean().sort_values(ascending=False).head(10).plot(kind="bar", ax=ax)
ax.set_title("Top 10 vị trí có lương trung bình cao nhất")
st.pyplot(fig)

# OVA trung bình theo CLB (Top 10)
fig, ax = plt.subplots()
df.groupby("Club")["OVA"].mean().sort_values(ascending=False).head(10).plot(kind="bar", ax=ax)
ax.set_title("Top 10 CLB có OVA trung bình cao nhất")
st.pyplot(fig)

# Scatter plot: Tuổi vs OVA
fig, ax = plt.subplots()
ax.scatter(df["Age"], df["OVA"], alpha=0.5)
ax.set_xlabel("Age")
ax.set_ylabel("OVA")
ax.set_title("Tương quan giữa Tuổi và OVA")
st.pyplot(fig)
