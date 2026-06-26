import sqlite3
import pandas as pd
import os

# Setup paths
base_path = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_path, 'retailflow.db')
sql_path = os.path.join(base_path, 'sql', 'analytics.sql')

if not os.path.exists(db_path):
    print(f"Error: Database not found at {db_path}")
    exit()

if not os.path.exists(sql_path):
    print(f"Error: SQL file not found at {sql_path}")
    exit()

conn = sqlite3.connect(db_path)

# Read analytics.sql
with open(sql_path, "r", encoding="utf-8") as f:
    sql_text = f.read()

# Split queries by semicolon
raw_queries = [q.strip() for q in sql_text.split(";") if q.strip()]

kpi_names = [
    "Revenue by Month",
    "Top Sellers",
    "Average Delivery Time (Days by State)",
    "Orders by State"
]

print("\n=========================================")
print("Executing RetailFlow KPI Queries")
print("=========================================\n")

for i, query in enumerate(raw_queries):
    name = kpi_names[i] if i < len(kpi_names) else f"Query {i+1}"
    print(f"--- KPI {i+1}: {name} ---")
    try:
        df = pd.read_sql_query(query, conn)
        # Display up to 10 rows
        print(df.head(10))
    except Exception as e:
        print(f"Error running query: {e}")
        print(f"Query was:\n{query}")
    print("-" * 50 + "\n")

conn.close()
