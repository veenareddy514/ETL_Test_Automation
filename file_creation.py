import csv
import pandas as pd
# ------------------------
# Customer Source Data
# ------------------------
customer_source_data = [
    ["customer_id", "first_name", "last_name", "email", "phone_number", "address", "city", "state", "zip_code"],
    [1, "John", "Doe", "john@example.com", "1234567890", "123 Main St", "San Jose", "CA", 95123],
    [2, "Jane", "Smith", "jane@example.com", "2345678901", "456 Oak St", "Fremont", "CA", 94536],
    [3, "Alice", "Brown", "alice@example.com", "", "789 Pine St", "Sunnyvale", "CA", 94085],
    [3, "Bob", "Green", "bob@example.com", "3456789012", "101 Maple St", "Palo Alto", "CA", 94301],
    [5, "Charlie", "Lee", "charlie@example.com", "4567890123", "202 Elm St", "Mountain View", "CA", 94040]]


#Write customer source data
with open("customer_src.csv","w",newline="") as f:
    writer=csv.writer(f)
    writer.writerows(customer_source_data)



# ------------------------
# Customer Staging Data (apply some transformations)
# ------------------------
customer_staging_df = pd.DataFrame(customer_source_data[1:], columns=customer_source_data[0])
# Add batch_date for incremental load simulation
customer_staging_df["batch_date"] = "2025-12-17"
customer_staging_df.to_csv("customer_staging.csv", index=False)

# ------------------------
# Customer Target (SCD Type 1) → Only latest value per customer
# ------------------------
# Load 1 (historical)
cust_target_load1 = customer_staging_df.copy()
cust_target_load1["batch_date"] = "2025-12-15"
cust_target_load1.loc[cust_target_load1["customer_id"] == 2, "email"] = "jane.smith@example.com"
cust_target_load1.loc[cust_target_load1["customer_id"] == 5, "phone_number"] = "4560000000"

# Load 2 (overwrite updates)
cust_target_load2 = customer_staging_df.copy()
cust_target_load2["batch_date"] = "2025-12-16"
cust_target_load2.loc[cust_target_load2["customer_id"] == 1, "email"] = "john.new@example.com"

# Load 3 (current batch)
cust_target_load3 = customer_staging_df.copy()
cust_target_load3["batch_date"] = "2025-12-17"
cust_target_load3.loc[cust_target_load3["customer_id"] == 4, "phone_number"] = "9998887777"

# Combine all customer target loads
customer_target_df = pd.concat([cust_target_load1, cust_target_load2, cust_target_load3], ignore_index=True)
customer_target_df.to_csv("customer_target.csv", index=False)


# ------------------------
# Orders Source Data (SCD Type 2)
# ------------------------
orders_source_data = [
    {"order_id": 101, "customer_id": 1, "order_date": "2025-08-20", "amount": 250},
    {"order_id": 102, "customer_id": 2, "order_date": "2025-08-21", "amount": 450},
    {"order_id": 103, "customer_id": 3, "order_date": "2025-08-22", "amount": 300},
    {"order_id": 104, "customer_id": 4, "order_date": "2025-08-23", "amount": 500},
    {"order_id": 105, "customer_id": 5, "order_date": "2025-08-24", "amount": 600},
]

# ------------------------
# Orders Source Data (SCD Type 2)
# ------------------------

orders_src_df = pd.DataFrame(orders_source_data)
orders_src_df.to_csv("orders_source.csv", index=False)

# ------------------------
# Orders Staging Data (SCD Type 2)
# ------------------------
orders_staging_df = orders_src_df.copy()
orders_staging_df["batch_date"] = "2025-12-17"
orders_staging_df.to_csv("orders_staging.csv", index=False)

# ------------------------
# Orders Target (SCD Type 2)
# ------------------------

# Helper function to create SCD2 records
def create_scd2_load(stg_df, batch_date, changes=None):
    df = stg_df.copy()
    df["effective_start_date"] = batch_date
    df["effective_end_date"] = None
    df["is_active"] = "Y"

    if changes:
        for order_id, new_amount in changes.items():
            df.loc[df["order_id"] == order_id, "amount"] = new_amount

    return df


# ---------- Load 1 ----------
orders_target_load1 = create_scd2_load(
    orders_staging_df,
    batch_date="2025-12-15"
)

# ---------- Load 2 (Type 2 change for order_id 103) ----------
orders_target_load2 = create_scd2_load(
    orders_staging_df,
    batch_date="2025-12-16",
    changes={103: 310}
)

# Close previous active record for order_id = 103
orders_target_load1.loc[
    orders_target_load1["order_id"] == 103,
    ["effective_end_date", "is_active"]
] = ["2025-12-15", "N"]


# ---------- Load 3 (Type 2 change for order_id 105) ----------
orders_target_load3 = create_scd2_load(
    orders_staging_df,
    batch_date="2025-12-17",
    changes={105: 620}
)

# Close previous active record for order_id = 105
orders_target_load2.loc[
    orders_target_load2["order_id"] == 105,
    ["effective_end_date", "is_active"]
] = ["2025-12-16", "N"]


# ---------- Combine all loads ----------
orders_target_df = pd.concat(
    [orders_target_load1, orders_target_load2, orders_target_load3],
    ignore_index=True
)

# Save final SCD2 target
orders_target_df.to_csv("orders_target.csv", index=False)

print("orders_target.csv created with SCD Type 2 logic!")
print("Dataset creation complete:")
print("- Customer → SCD Type 1")
print("- Orders → SCD Type 2 with is_active flag")



