# etl/validate.py
import os
import pandas as pd
from etl.config import ERROR_DATA_PATH, VALIDATION_SCHEMAS, logger

def validate_chunk(df, table_name):
    """
    Validates a chunk of data against rules defined in VALIDATION_SCHEMAS.
    Quarantines invalid rows to data/errors/<table_name>_quarantine.csv.
    Returns: (valid_df, total_count, valid_count, invalid_count)
    """
    if df.empty:
        return df, 0, 0, 0

    total_count = len(df)
    schema = VALIDATION_SCHEMAS.get(table_name, {})
    required_cols = schema.get("required_columns", [])
    
    # Initialize mask: True means row is valid
    is_valid = pd.Series(True, index=df.index)
    reasons = pd.Series("", index=df.index)

    # 1. Validate Required (Non-null) Columns
    for col in required_cols:
        if col in df.columns:
            null_mask = df[col].isnull()
            is_valid = is_valid & (~null_mask)
            reasons.loc[null_mask & (reasons == "")] = f"Missing required column: {col}"
        else:
            is_valid = pd.Series(False, index=df.index)
            reasons = pd.Series(f"Missing column from schema: {col}", index=df.index)
            break

    # 2. Schema-specific constraints
    if table_name in ["customers", "sellers"]:
        state_col = "customer_state" if table_name == "customers" else "seller_state"
        state_len = schema.get("state_len", 2)
        if state_col in df.columns:
            # Check length of state string
            invalid_state = df[state_col].astype(str).str.strip().str.len() != state_len
            # Keep rows where state length is valid and not already invalid
            is_valid = is_valid & (~invalid_state)
            reasons.loc[invalid_state & (reasons == "")] = f"Invalid state length (must be {state_len})"

    elif table_name == "items":
        min_price = schema.get("min_price", 0.0)
        if "price" in df.columns:
            invalid_price = pd.to_numeric(df["price"], errors='coerce').isnull() | (df["price"] < min_price)
            is_valid = is_valid & (~invalid_price)
            reasons.loc[invalid_price & (reasons == "")] = f"Price less than {min_price} or invalid"

    elif table_name == "payments":
        min_payment = schema.get("min_payment", 0.0)
        if "payment_value" in df.columns:
            invalid_payment = pd.to_numeric(df["payment_value"], errors='coerce').isnull() | (df["payment_value"] < min_payment)
            is_valid = is_valid & (~invalid_payment)
            reasons.loc[invalid_payment & (reasons == "")] = f"Payment value less than {min_payment} or invalid"

    # Separate valid and invalid DataFrames
    valid_df = df[is_valid].copy()
    invalid_df = df[~is_valid].copy()

    valid_count = len(valid_df)
    invalid_count = len(invalid_df)

    # Quarantine invalid records if any exist
    if invalid_count > 0:
        invalid_df["quarantine_reason"] = reasons[~is_valid]
        quarantine_file = os.path.join(ERROR_DATA_PATH, f"{table_name}_quarantine.csv")
        
        # Write/Append to CSV
        file_exists = os.path.exists(quarantine_file)
        try:
            invalid_df.to_csv(quarantine_file, mode="a", index=False, header=not file_exists, encoding="utf-8")
            logger.warning(f"Quarantined {invalid_count} invalid records for '{table_name}' to {quarantine_file}")
        except Exception as e:
            logger.error(f"Failed to write quarantine file for '{table_name}': {e}")

    return valid_df, total_count, valid_count, invalid_count

if __name__ == "__main__":
    # Test validation
    test_data = pd.DataFrame([
        {"customer_id": "c1", "customer_unique_id": "cu1", "customer_state": "SP"},
        {"customer_id": None, "customer_unique_id": "cu2", "customer_state": "RJ"}, # Invalid: missing customer_id
        {"customer_id": "c3", "customer_unique_id": "cu3", "customer_state": "S"},   # Invalid: state length
    ])
    
    valid_df, total, valid, invalid = validate_chunk(test_data, "customers")
    print(f"Total: {total}, Valid: {valid}, Invalid: {invalid}")
    print("\n--- Valid Data ---")
    print(valid_df)
