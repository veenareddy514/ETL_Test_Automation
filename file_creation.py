import csv

# ------------------------
# Sample Source Data
# ------------------------
source_data = [
    ["customer_id", "first_name", "last_name", "email", "phone_number", "address", "city", "state", "zip_code"],
    [1, "John", "Doe", "john@example.com", "1234567890", "123 Main St", "San Jose", "CA", 95123],
    [2, "Jane", "Smith", "jane@example.com", "2345678901", "456 Oak St", "Fremont", "CA", 94536],
    [3, "Alice", "Brown", "alice@example.com", "", "789 Pine St", "Sunnyvale", "CA", 94085],  # Phone missing (null)
    [3, "Alice", "Brown", "alice@example.com", "", "789 Pine St", "Sunnyvale", "CA", 94085],  # Duplicate PK
    [4, "Bob", "Green", "bob@example.com", "3456789012", "101 Maple St", "Palo Alto", "CA", 94301],
]

# Write source CSV
with open("source.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(source_data)

# ------------------------
# Sample Target Data
# ------------------------
target_data = [
    ["customer_id", "first_name", "last_name", "email", "phone_number", "address", "city", "state", "zip_code"],
    [1, "John", "Doe", "john@example.com", "1234567890", "123 Main St", "San Jose", "CA", 95123],
    [2, "Jane", "Smith", "jane.smith@example.com", "2345678901", "456 Oak St", "Fremont", "CA", 94536],  # Transformation change in email
    [3, "Alice", "Brown", "alice@example.com", "", "789 Pine St", "Sunnyvale", "CA", 94085],
    ['', "Bob", "Green", "bob@example.com", "3456789012", "101 Maple St", "Palo Alto", "CA", 94301],
    [None, "Charlie", "", "charlie@example.com", "4567890123", "202 Elm St", "Mountain View", "CA", 94040],  # Extra row in target
]

# Write target CSV
with open("target.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(target_data)

print("Sample source.csv and target.csv files created!")


import pandas as pd

# -----------------------
# Source Data (orders_src.csv)
# -----------------------
source_data = [
    {"order_id": 101, "customer_id": 1, "order_date": "2025-08-20", "amount": 250},
    {"order_id": 102, "customer_id": 2, "order_date": "2025-08-21", "amount": 450},
    {"order_id": 103, "customer_id": 3, "order_date": "2025-08-22", "amount": 300},
    {"order_id": 104, "customer_id": 4, "order_date": "2025-08-23", "amount": None},  # Null value
    {"order_id": 105, "customer_id": 5, "order_date": "2025-08-24", "amount": 500},
    {"order_id": 102, "customer_id": 2, "order_date": "2025-08-21", "amount": 450}  # Duplicate
]

src_df = pd.DataFrame(source_data)
src_df.to_csv("orders_src.csv", index=False)
print("orders_src.csv created successfully!")

# -----------------------
# Target Data (orders_trgt.csv)
# -----------------------
# Introduce slight differences: missing record, changed amount, extra record
target_data = [
    {"order_id": 101, "customer_id": 1, "order_date": "2025-08-20", "amount": 250},
    {"order_id": 102, "customer_id": 2, "order_date": "2025-08-21", "amount": 460},  # Changed amount
    {"order_id": 103, "customer_id": 3, "order_date": "2025-08-22", "amount": 300},
    {"order_id": 105, "customer_id": 5, "order_date": "2025-08-24", "amount": 500},
    {"order_id": 106, "customer_id": 6, "order_date": "2025-08-25", "amount": 600}  # Extra record
]

trgt_df = pd.DataFrame(target_data)
trgt_df.to_csv("orders_trgt.csv", index=False)
print("orders_trgt.csv created successfully!")
