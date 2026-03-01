from sqlalchemy import create_engine
import pandas as pd

# ----- DATABASE CONNECTION -----
DB_USER = "postgres"
DB_PASSWORD = "19996456"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce_analytics"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ----- LOAD DATA FROM SQL VIEW -----
query = "SELECT * FROM v_transactions_clean"

df = pd.read_sql(query, engine)

print("Rows loaded:", len(df))
print(df.head())

import numpy as np

print("Rows loaded:", len(df))
print(df.head(3))

# 1) Check dtypes
print("\n--- dtypes ---")
print(df.dtypes)

# 2) Convert transaction_date to datetime (if needed)
df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

# 3) Null checks (same idea as SQL)
print("\n--- nulls ---")
important_cols = ["transaction_id", "transaction_date", "amount_spent", "segment", "state_name", "payment_method"]
print(df[important_cols].isna().sum())

# 4) Basic KPIs (should feel close to your SQL outputs)
total_transactions = df["transaction_id"].nunique()
total_revenue = df["amount_spent"].sum()
aov = df["amount_spent"].mean()

print("\n--- KPIs ---")
print("Total transactions (distinct):", total_transactions)
print("Total revenue:", round(total_revenue, 2))
print("Avg order value (mean):", round(aov, 2))

# 5) Monthly revenue table (like your v_monthly_revenue_growth base)
df["month"] = df["transaction_date"].dt.to_period("M").dt.to_timestamp()

monthly = (
    df.groupby("month", as_index=False)
      .agg(total_transactions=("transaction_id", "nunique"),
           total_revenue=("amount_spent", "sum"),
           avg_order_value=("amount_spent", "mean"))
      .sort_values("month")
)

monthly["previous_month_revenue"] = monthly["total_revenue"].shift(1)
monthly["revenue_growth_pct"] = (monthly["total_revenue"] / monthly["previous_month_revenue"] - 1) * 100

print("\n--- Monthly (first 6 rows) ---")
print(monthly.head(6))

import matplotlib.pyplot as plt

fig, ax1 = plt.subplots(figsize=(12,6))

# Revenue line
ax1.plot(monthly["month"], monthly["total_revenue"])
ax1.set_xlabel("Month")
ax1.set_ylabel("Total Revenue")
ax1.set_title("Monthly Revenue & Growth Trend")

# Second axis for growth
ax2 = ax1.twinx()
ax2.plot(monthly["month"], monthly["revenue_growth_pct"])
ax2.set_ylabel("Revenue Growth %")

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

monthly["rolling_3m_revenue"] = monthly["total_revenue"].rolling(3).mean()
fig, ax1 = plt.subplots(figsize=(12,6))

# Revenue
ax1.plot(monthly["month"], monthly["total_revenue"], label="Revenue")

# Rolling average
ax1.plot(monthly["month"], monthly["rolling_3m_revenue"], 
         linewidth=3, label="3M Rolling Avg")

ax1.set_xlabel("Month")
ax1.set_ylabel("Total Revenue")
ax1.set_title("Monthly Revenue & Growth Trend")

ax2 = ax1.twinx()
ax2.plot(monthly["month"], monthly["revenue_growth_pct"], label="Growth %")
ax2.set_ylabel("Revenue Growth %")

ax1.legend(loc="upper left")

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Remove first month (NaN growth)
growth_clean = monthly.dropna(subset=["revenue_growth_pct"])

best_month = growth_clean.loc[growth_clean["revenue_growth_pct"].idxmax()]
worst_month = growth_clean.loc[growth_clean["revenue_growth_pct"].idxmin()]

print("\n--- Best Growth Month ---")
print(best_month[["month", "revenue_growth_pct"]])

print("\n--- Worst Growth Month ---")
print(worst_month[["month", "revenue_growth_pct"]])

print("\n--- Strategic Insight ---")

print(
    f"The strongest monthly growth occurred in {best_month['month'].strftime('%Y-%m')} "
    f"with a growth rate of {best_month['revenue_growth_pct']:.2f}%."
)

print(
    f"The largest revenue decline occurred in {worst_month['month'].strftime('%Y-%m')} "
    f"with a drop of {worst_month['revenue_growth_pct']:.2f}%."
)

monthly.to_csv("monthly_revenue_analysis.csv", index=False)