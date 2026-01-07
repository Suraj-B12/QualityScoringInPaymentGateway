import sys
import os
import pandas as pd
sys.path.append(os.getcwd())

from src.data_generator import generate_visa_transactions
from src.models.schema import parse_visa_transaction

def generate_mixed_csv():
    print("Generating 500 transactions with 50% anomaly rate...")
    # Generate transactions with 50% anomaly rate to achieve ~50% success rate
    transactions = generate_visa_transactions(
        n_transactions=500,
        anomaly_rate=0.50,
        random_seed=42
    )
    
    print("Flattening transactions...")
    flat_data = []
    for txn_dict in transactions:
        # Parse into Pydantic model and then flatten
        txn_obj = parse_visa_transaction(txn_dict)
        flat_data.append(txn_obj.flatten())
    
    # Create DataFrame
    df = pd.DataFrame(flat_data)
    
    # Ensure output directory exists
    os.makedirs("data", exist_ok=True)
    output_path = os.path.join("data", "mixed_quality_data.csv")
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} records to {output_path}")
    print(f"File size: {os.path.getsize(output_path) / 1024:.2f} KB")

if __name__ == "__main__":
    generate_mixed_csv()
