"""
Live Data Generator for Real-Time Streaming

Generates realistic VISA transaction data matching the expected schema.
Can be configured with an external API key for production use.
"""
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import json
import os


class LiveDataGenerator:
    """
    Generates realistic transaction data for live streaming.
    
    In production, replace generate_transaction() with API fetch.
    """
    
    # Configuration
    NETWORKS = ["VISA", "Mastercard", "RuPay", "AMEX"]
    CARD_TYPES = ["credit", "debit", "prepaid"]
    FUNDING_SOURCES = ["consumer", "commercial", "prepaid"]
    ISSUER_BANKS = ["HDFC Bank", "ICICI Bank", "SBI", "Axis Bank", "Kotak", "Yes Bank"]
    ACQUIRER_BANKS = ["Axis Bank", "HDFC Bank", "ICICI Bank", "RBL Bank"]
    
    MERCHANT_NAMES = [
        "Amazon India", "Flipkart", "Swiggy", "Zomato", "BookMyShow",
        "MakeMyTrip", "Uber India", "Ola Cabs", "Big Bazaar", "Reliance Digital",
        "Croma", "Myntra", "Nykaa", "PharmEasy", "1mg", "Urban Company"
    ]
    
    MCC_CODES = {
        "5812": "Restaurants",
        "5411": "Grocery Stores", 
        "5541": "Service Stations",
        "5311": "Department Stores",
        "4111": "Transportation",
        "5999": "Miscellaneous Retail",
        "7011": "Hotels",
        "4722": "Travel Agencies",
        "5621": "Women's Clothing",
        "5732": "Electronics",
    }
    
    CITIES = [
        ("Bengaluru", "KA", "560001"),
        ("Mumbai", "MH", "400001"),
        ("Delhi", "DL", "110001"),
        ("Chennai", "TN", "600001"),
        ("Hyderabad", "TS", "500001"),
        ("Pune", "MH", "411001"),
        ("Kolkata", "WB", "700001"),
        ("Ahmedabad", "GJ", "380001"),
    ]
    
    HIGH_RISK_COUNTRIES = ["NG", "RU", "KP", "AF", "PK"]
    
    def __init__(self, api_key: Optional[str] = None, anomaly_rate: float = 0.15):
        """
        Initialize the generator.
        
        Args:
            api_key: Optional API key for external data source
            anomaly_rate: Rate of anomalous transactions (0.0 - 1.0)
        """
        self.api_key = api_key
        self.anomaly_rate = anomaly_rate
        self.transaction_count = 0
        
    def set_api_key(self, api_key: str):
        """Set the API key for external data source."""
        self.api_key = api_key
        
    def set_anomaly_rate(self, rate: float):
        """Set the anomaly rate (0.0 - 1.0)."""
        self.anomaly_rate = max(0.0, min(1.0, rate))
    
    def generate_transaction(self) -> Dict[str, Any]:
        """
        Generate a single realistic transaction.
        
        In production, this would fetch from an external API using self.api_key.
        For now, generates simulated data.
        """
        self.transaction_count += 1
        is_anomaly = random.random() < self.anomaly_rate
        
        # Generate unique IDs
        txn_id = f"txn_{uuid.uuid4().hex[:12].upper()}"
        order_id = f"order_{random.randint(10000, 99999)}"
        
        # Timestamp (current time with slight variation)
        timestamp = datetime.utcnow() - timedelta(seconds=random.randint(0, 5))
        
        # Amount - higher for anomalies
        if is_anomaly and random.random() < 0.5:
            amount = random.randint(50000, 500000)  # High value
        else:
            amount = random.choice([
                random.randint(100, 1000),      # Small
                random.randint(1000, 5000),     # Medium
                random.randint(5000, 20000),    # Large
            ])
        
        # Status
        if is_anomaly and random.random() < 0.3:
            status = random.choice(["declined", "failed", "pending"])
            response_code = random.choice(["05", "51", "14", "54"])
        else:
            status = "approved"
            response_code = "00"
        
        # Card details
        network = random.choice(self.NETWORKS)
        bin_number = random.choice(["411111", "422222", "433333", "511111", "522222", "653333"])
        
        # Merchant
        city, state, postal = random.choice(self.CITIES)
        country = "IN"
        if is_anomaly and random.random() < 0.4:
            country = random.choice(self.HIGH_RISK_COUNTRIES)
            city = "Unknown"
            state = "XX"
        
        mcc = random.choice(list(self.MCC_CODES.keys()))
        
        # Risk score
        if is_anomaly:
            risk_score = random.randint(60, 99)
            risk_level = "high" if risk_score > 80 else "medium"
        else:
            risk_score = random.randint(5, 40)
            risk_level = "low"
        
        # Velocity/geo checks
        velocity_check = "fail" if (is_anomaly and random.random() < 0.4) else "pass"
        geo_check = "fail" if (is_anomaly and random.random() < 0.3) else "pass"
        
        transaction = {
            "transaction": {
                "transaction_id": txn_id,
                "merchant_order_id": order_id,
                "type": "authorization",
                "amount": amount,
                "currency": "INR",
                "timestamp": timestamp.isoformat() + "Z",
                "status": status,
                "response_code": response_code,
                "authorization_code": f"A{random.randint(10000, 99999)}" if status == "approved" else None
            },
            "card": {
                "network": network,
                "pan_token": f"tok_{uuid.uuid4().hex[:12]}",
                "bin": bin_number,
                "last4": str(random.randint(1000, 9999)),
                "expiry_month": f"{random.randint(1, 12):02d}",
                "expiry_year": str(random.randint(2025, 2030)),
                "card_type": random.choice(self.CARD_TYPES),
                "funding_source": random.choice(self.FUNDING_SOURCES),
                "issuer_bank": random.choice(self.ISSUER_BANKS)
            },
            "merchant": {
                "merchant_id": f"MID_{random.randint(1000, 9999)}",
                "terminal_id": f"TID_{random.randint(1000, 9999)}",
                "merchant_name": random.choice(self.MERCHANT_NAMES),
                "merchant_category_code": mcc,
                "country": country,
                "acquirer_bank": random.choice(self.ACQUIRER_BANKS),
                "settlement_account": f"XXXXXX{random.randint(1000, 9999)}"
            },
            "customer": {
                "customer_id": f"cust_{uuid.uuid4().hex[:8]}",
                "email": f"user{random.randint(100, 999)}@example.com" if random.random() > 0.1 else None,
                "phone": f"+91{random.randint(7000000000, 9999999999)}" if random.random() > 0.1 else None,
                "billing_address": {
                    "city": city,
                    "state": state,
                    "country": country,
                    "postal_code": postal
                },
                "shipping_address": {
                    "city": city,
                    "state": state,
                    "country": country,
                    "postal_code": postal
                },
                "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                "device_fingerprint": f"fp_{uuid.uuid4().hex[:12]}",
                "user_agent": random.choice(["Chrome/Windows", "Safari/MacOS", "Firefox/Linux", "Chrome/Android", "Safari/iOS"])
            },
            "authentication": {
                "three_ds_version": random.choice(["2.1", "2.2", "1.0"]),
                "eci": random.choice(["05", "06", "07"]),
                "cavv": f"{''.join(random.choices(string.ascii_uppercase, k=9))}",
                "ds_transaction_id": f"ds_{uuid.uuid4().hex[:12]}",
                "authentication_result": "authenticated" if random.random() > 0.1 else "failed"
            },
            "fraud": {
                "risk_score": risk_score,
                "risk_level": risk_level,
                "velocity_check": velocity_check,
                "geo_check": geo_check
            },
            "network": {
                "network_transaction_id": f"net_{uuid.uuid4().hex[:12]}",
                "acquirer_reference_number": f"ARN_{random.randint(100000000000, 999999999999)}",
                "routing_region": "APAC",
                "interchange_category": "consumer_credit"
            },
            "compliance": {
                "sca_applied": random.choice([True, False]),
                "psd2_exemption": None,
                "aml_screening": "clear" if random.random() > 0.05 else "review",
                "tax_reference": f"GST_{uuid.uuid4().hex[:8].upper()}",
                "audit_log_id": f"audit_{uuid.uuid4().hex[:12]}"
            },
            "settlement": {
                "settlement_batch_id": f"batch_{uuid.uuid4().hex[:8]}",
                "clearing_date": (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d"),
                "settlement_date": (datetime.utcnow() + timedelta(days=2)).strftime("%Y-%m-%d"),
                "gross_amount": amount,
                "interchange_fee": int(amount * 0.008),
                "gateway_fee": int(amount * 0.003),
                "net_amount": amount - int(amount * 0.011)
            },
            "business_metadata": {
                "invoice_number": f"INV_{random.randint(10000, 99999)}",
                "product_category": random.choice(["Electronics", "Fashion", "Food", "Travel", "Entertainment", "Services"]),
                "promo_code": random.choice([None, "NEWUSER", "SAVE10", "SPECIAL"]),
                "campaign": random.choice([None, "HolidaySale", "Weekend", "Flash"]),
                "notes": None
            },
            "_metadata": {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "is_simulated": True,
                "sequence_number": self.transaction_count
            }
        }
        
        return transaction
    
    def flatten_for_dqs(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested transaction to match DQS engine input format.
        """
        txn = transaction.get("transaction", {})
        card = transaction.get("card", {})
        merchant = transaction.get("merchant", {})
        customer = transaction.get("customer", {})
        fraud = transaction.get("fraud", {})
        auth = transaction.get("authentication", {})
        settlement = transaction.get("settlement", {})
        
        return {
            "transaction_id": txn.get("transaction_id"),
            "amount": txn.get("amount"),
            "currency": txn.get("currency"),
            "timestamp": txn.get("timestamp"),
            "status": txn.get("status"),
            "network": card.get("network"),
            "card_type": card.get("card_type"),
            "bin": card.get("bin"),
            "last4": card.get("last4"),
            "merchant_id": merchant.get("merchant_id"),
            "merchant_category_code": merchant.get("merchant_category_code"),
            "country": merchant.get("country"),
            "customer_id": customer.get("customer_id"),
            "email": customer.get("email"),
            "phone": customer.get("phone"),
            "billing_city": customer.get("billing_address", {}).get("city"),
            "billing_country": customer.get("billing_address", {}).get("country"),
            "ip_address": customer.get("ip_address"),
            "risk_score": fraud.get("risk_score"),
            "risk_level": fraud.get("risk_level"),
            "velocity_check": fraud.get("velocity_check"),
            "geo_check": fraud.get("geo_check"),
            "auth_result": auth.get("authentication_result"),
            "settlement_fee": settlement.get("interchange_fee", 0) + settlement.get("gateway_fee", 0),
            "net_amount": settlement.get("net_amount"),
        }
import threading


class LiveLogStorage:
    """
    Persistent storage for live stream logs.
    Stores logs to a JSON file for persistence across restarts.
    Thread-safe for concurrent access.
    """
    
    def __init__(self, log_file: str = "live_stream_logs.json"):
        self.log_file = log_file
        self.logs = []
        self._lock = threading.Lock()
        self._load_logs()
    
    def _load_logs(self):
        """Load existing logs from file."""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r') as f:
                    self.logs = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.logs = []
    
    def _save_logs(self):
        """Save logs to file (must be called with lock held)."""
        try:
            with open(self.log_file, 'w') as f:
                json.dump(self.logs, f, indent=2, default=str)
        except IOError:
            pass
    
    def add_log(self, transaction: Dict[str, Any], result: Dict[str, Any]):
        """Add a processed transaction log (thread-safe)."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "transaction_id": transaction.get("transaction", {}).get("transaction_id"),
            "amount": transaction.get("transaction", {}).get("amount"),
            "status": transaction.get("transaction", {}).get("status"),
            "dqs_score": result.get("dqs_score", 0),
            "action": result.get("action", "UNKNOWN"),
            "flags": result.get("flags", []),
            "processing_time_ms": result.get("processing_time_ms", 0),
            "full_transaction": transaction,
            "full_result": result
        }
        with self._lock:
            self.logs.append(log_entry)
            # Save every 10 logs to reduce I/O
            if len(self.logs) % 10 == 0:
                self._save_logs()
    
    def get_logs(self, start_time: str = None, end_time: str = None) -> list:
        """Get logs filtered by time range (thread-safe)."""
        with self._lock:
            if not start_time and not end_time:
                return list(self.logs)  # Return a copy
            
            filtered = []
            for log in self.logs:
                log_time = log.get("timestamp", "")
                
                if start_time and log_time < start_time:
                    continue
                if end_time and log_time > end_time:
                    continue
                
                filtered.append(log)
            
            return filtered
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics (thread-safe)."""
        with self._lock:
            if not self.logs:
                return {
                    "total": 0,
                    "safe": 0,
                    "review": 0,
                    "escalate": 0,
                    "rejected": 0,
                    "avg_dqs": 0,
                }
            
            safe = sum(1 for l in self.logs if l.get("action") == "SAFE_TO_USE")
            review = sum(1 for l in self.logs if l.get("action") == "REVIEW_REQUIRED")
            escalate = sum(1 for l in self.logs if l.get("action") == "ESCALATE")
            rejected = sum(1 for l in self.logs if l.get("action") == "NO_ACTION")
            
            dqs_scores = [l.get("dqs_score", 100) for l in self.logs]
            avg_dqs = sum(dqs_scores) / len(dqs_scores) if dqs_scores else 0
            
            return {
                "total": len(self.logs),
                "safe": safe,
                "review": review,
                "escalate": escalate,
                "rejected": rejected,
                "avg_dqs": round(avg_dqs, 1),
            }
    
    def clear_logs(self):
        """Clear all logs (thread-safe)."""
        with self._lock:
            self.logs = []
            self._save_logs()
    
    def flush(self):
        """Force save logs to disk (thread-safe)."""
        with self._lock:
            self._save_logs()
