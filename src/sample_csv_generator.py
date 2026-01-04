"""
Sample CSV Generator

Creates sample CSV files with varying quality levels for testing the DQS Engine.
Generates files following the required VISA schema structure.
"""
import csv
import os
import random
from datetime import datetime, timedelta


def generate_sample_csvs(output_dir: str = "sample_data"):
    """Generate sample CSV files with different quality levels."""
    os.makedirs(output_dir, exist_ok=True)
    
    # High quality (95%+ quality rate expected)
    generate_high_quality_csv(os.path.join(output_dir, "high_quality_transactions.csv"), 100)
    
    # Medium quality (60-80% quality rate expected)
    generate_medium_quality_csv(os.path.join(output_dir, "medium_quality_transactions.csv"), 100)
    
    # Low quality (30-50% quality rate expected)
    generate_low_quality_csv(os.path.join(output_dir, "low_quality_transactions.csv"), 100)
    
    # Very low quality - non-standard format (for testing adapter)
    generate_nonstandard_csv(os.path.join(output_dir, "nonstandard_pos_transactions.csv"), 100)
    
    print(f"Generated sample CSV files in {output_dir}/")
    return output_dir


def generate_high_quality_csv(filepath: str, count: int):
    """Generate high-quality transactions with minimal anomalies."""
    headers = [
        "transaction_id", "amount", "currency", "timestamp", "status",
        "network", "card_type", "bin", "last4",
        "merchant_id", "merchant_category_code", "country",
        "customer_id", "email", "phone",
        "risk_score", "risk_level"
    ]
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for i in range(count):
            base_time = datetime.now() - timedelta(days=random.randint(0, 30))
            amount = round(random.uniform(500, 15000), 2)
            
            row = {
                "transaction_id": f"txn_{i:08d}",
                "amount": amount,
                "currency": "INR",
                "timestamp": base_time.isoformat(),
                "status": "approved",
                "network": random.choice(["VISA", "Mastercard", "RuPay"]),
                "card_type": random.choice(["credit", "debit"]),
                "bin": str(random.randint(400000, 499999)),
                "last4": str(random.randint(1000, 9999)),
                "merchant_id": f"MID_{random.randint(1000, 9999)}",
                "merchant_category_code": random.choice(["5812", "5411", "5541", "5311"]),
                "country": "IN",
                "customer_id": f"cust_{random.randint(1000, 9999)}",
                "email": f"user{i}@example.com",
                "phone": f"+91{random.randint(7000000000, 9999999999)}",
                "risk_score": random.randint(5, 30),
                "risk_level": "low"
            }
            writer.writerow(row)
    
    print(f"  Created: {filepath} ({count} records, ~95% quality)")


def generate_medium_quality_csv(filepath: str, count: int):
    """Generate medium-quality transactions with some issues."""
    headers = [
        "transaction_id", "amount", "currency", "timestamp", "status",
        "network", "card_type", "bin", "last4",
        "merchant_id", "merchant_category_code", "country",
        "customer_id", "email", "phone",
        "risk_score", "risk_level"
    ]
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for i in range(count):
            base_time = datetime.now() - timedelta(days=random.randint(0, 60))
            is_issue = i % 4 == 0  # 25% with issues
            
            amount = round(random.uniform(100000, 500000) if is_issue else random.uniform(500, 20000), 2)
            
            row = {
                "transaction_id": f"txn_{i:08d}",
                "amount": amount,
                "currency": "INR" if not is_issue else random.choice(["INR", "USD", "EUR"]),
                "timestamp": base_time.isoformat(),
                "status": "approved" if not is_issue else random.choice(["approved", "declined", "pending"]),
                "network": random.choice(["VISA", "Mastercard", "RuPay", "UNKNOWN"]) if is_issue else random.choice(["VISA", "Mastercard"]),
                "card_type": random.choice(["credit", "debit", "prepaid"]),
                "bin": str(random.randint(400000, 499999)),
                "last4": str(random.randint(1000, 9999)),
                "merchant_id": f"MID_{random.randint(1000, 9999)}",
                "merchant_category_code": random.choice(["5812", "5411", "0000"]) if is_issue else "5812",
                "country": random.choice(["IN", "US", "GB", "NG"]) if is_issue else "IN",
                "customer_id": f"cust_{random.randint(1000, 9999)}",
                "email": f"user{i}@example.com" if not is_issue else "",
                "phone": f"+91{random.randint(7000000000, 9999999999)}" if not is_issue else "",
                "risk_score": random.randint(40, 80) if is_issue else random.randint(10, 40),
                "risk_level": "high" if is_issue else "low"
            }
            writer.writerow(row)
    
    print(f"  Created: {filepath} ({count} records, ~65% quality)")


def generate_low_quality_csv(filepath: str, count: int):
    """Generate low-quality transactions with many anomalies."""
    headers = [
        "transaction_id", "amount", "currency", "timestamp", "status",
        "network", "card_type", "bin", "last4",
        "merchant_id", "merchant_category_code", "country",
        "customer_id", "email", "phone",
        "risk_score", "risk_level"
    ]
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for i in range(count):
            base_time = datetime.now() - timedelta(days=random.randint(0, 90))
            is_critical = i % 2 == 0  # 50% with critical issues
            
            # Intentionally create problematic data
            amount = round(random.uniform(500000, 2000000) if is_critical else random.uniform(500, 10000), 2)
            
            row = {
                "transaction_id": f"txn_{i:08d}",
                "amount": amount,
                "currency": random.choice(["INR", "XXX", "???", "USD"]),
                "timestamp": base_time.isoformat() if not is_critical else "invalid_date",
                "status": random.choice(["approved", "declined", "failed", "error"]),
                "network": random.choice(["UNKNOWN", "VISA", "NONE", ""]),
                "card_type": random.choice(["unknown", "credit", ""]),
                "bin": str(random.randint(400000, 499999)) if not is_critical else "000000",
                "last4": str(random.randint(1000, 9999)) if not is_critical else "0000",
                "merchant_id": f"MID_{random.randint(1000, 9999)}" if not is_critical else "",
                "merchant_category_code": random.choice(["0000", "9999", ""]),
                "country": random.choice(["XX", "NG", "KP", ""]),
                "customer_id": f"cust_{random.randint(1000, 9999)}" if not is_critical else "",
                "email": "",
                "phone": "",
                "risk_score": random.randint(70, 100),
                "risk_level": "high"
            }
            writer.writerow(row)
    
    print(f"  Created: {filepath} ({count} records, ~35% quality)")


def generate_nonstandard_csv(filepath: str, count: int):
    """Generate non-standard format CSV (POS-style) to test adapter."""
    # Completely different column names - tests the adapter's mapping ability
    headers = [
        "TRANSACTION_ID", "CARD_ID", "AMOUNT", "TRANSACTION_TYPE", 
        "TIMESTAMP", "STORE_ID", "TERMINAL_ID", "REFERENCE_NUMBER"
    ]
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for i in range(count):
            base_time = datetime.now() - timedelta(days=random.randint(0, 30))
            
            row = {
                "TRANSACTION_ID": f"TX-POS-{base_time.strftime('%Y%m%d%H%M%S')}-{i:05d}",
                "CARD_ID": f"GC-STORE-{random.randint(1000, 9999):04d}-{random.randint(1000, 9999)}",
                "AMOUNT": round(random.uniform(100, 50000), 2),
                "TRANSACTION_TYPE": random.choice(["PURCHASE", "REFUND", "VOID"]),
                "TIMESTAMP": base_time.strftime("%Y-%m-%d %H:%M:%S"),
                "STORE_ID": f"STORE-{random.randint(100, 999):04d}",
                "TERMINAL_ID": f"POS-{random.randint(100, 999)}",
                "REFERENCE_NUMBER": f"REF-{random.randint(100000, 999999)}"
            }
            writer.writerow(row)
    
    print(f"  Created: {filepath} ({count} records, non-standard format)")


if __name__ == "__main__":
    generate_sample_csvs("sample_data")
