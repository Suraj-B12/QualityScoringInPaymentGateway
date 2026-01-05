"""
Flask API Server for DQS Engine Frontend

Provides REST endpoints to interact with the Data Quality Scoring Engine.
Supports standard VISA format and graceful handling of non-standard CSV/JSON.
"""
import os
import sys
import json
import math
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import numpy as np

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.dqs_engine import DQSEngine
from src.data_generator import generate_visa_transactions
from src.csv_adapter import adapt_csv_to_visa, adapt_flat_json_to_visa
from src.config import Action


class SafeJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles NaN, Infinity, and numpy types."""
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def sanitize_for_json(obj):
    """Recursively sanitize an object for JSON serialization."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return sanitize_for_json(obj.tolist())
    return obj


app = Flask(__name__, static_folder='frontend', static_url_path='')
app.json_encoder = SafeJSONEncoder
CORS(app)

# Global engine instance
engine = None


def get_engine(use_ai=False):
    """Get or create engine instance."""
    global engine
    api_key = os.environ.get("GEMINI_API_KEY", "")
    engine = DQSEngine(gemini_api_key=api_key, use_ai=use_ai and bool(api_key))
    return engine


@app.route('/')
def index():
    """Serve the main dashboard."""
    return send_from_directory('frontend', 'index.html')


@app.route('/api/health')
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "layers": 15,
        "phases": 7
    })


@app.route('/api/generate', methods=['POST'])
def generate_data():
    """Generate sample VISA transactions with preview."""
    try:
        data = request.get_json() or {}
        n_transactions = min(int(data.get('count', 20)), 500)  # Max 500
        anomaly_rate = min(float(data.get('anomaly_rate', 0.15)), 0.5)  # Max 50%
        seed = data.get('seed')
        
        transactions = generate_visa_transactions(
            n_transactions=n_transactions,
            anomaly_rate=anomaly_rate,
            random_seed=seed
        )
        
        # Return full transactions for preview
        return jsonify({
            "success": True,
            "count": n_transactions,
            "anomaly_rate": anomaly_rate,
            "preview": transactions[:20],  # Preview up to 20
            "transactions": transactions,  # Full data for execution
            "message": f"Generated {n_transactions} transactions"
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/run', methods=['POST'])
def run_pipeline():
    """Run the complete DQS pipeline with graceful CSV/JSON handling."""
    try:
        data = request.get_json() or {}
        
        # Check for various data sources
        custom_data = data.get('custom_data')
        csv_content = data.get('csv_content')
        use_ai = data.get('use_ai', False)
        
        import_metadata = None
        
        if csv_content:
            # CSV content provided - adapt to VISA format
            transactions, import_metadata = adapt_csv_to_visa(csv_content)
            if not transactions:
                return jsonify({
                    "success": False,
                    "error": "Failed to parse CSV content. Please check the format."
                }), 400
                
        elif custom_data:
            # JSON data provided - check if it needs adaptation
            if isinstance(custom_data, list):
                transactions, import_metadata = adapt_flat_json_to_visa(custom_data)
            else:
                transactions, import_metadata = adapt_flat_json_to_visa([custom_data])
        else:
            # Generate transactions
            n_transactions = min(int(data.get('count', 20)), 500)
            anomaly_rate = min(float(data.get('anomaly_rate', 0.15)), 0.5)
            seed = data.get('seed', 42)
            
            transactions = generate_visa_transactions(
                n_transactions=n_transactions,
                anomaly_rate=anomaly_rate,
                random_seed=seed
            )
        
        if not transactions:
            return jsonify({
                "success": False,
                "error": "No valid transactions to process"
            }), 400
        
        # Run engine with error handling that preserves import metadata
        try:
            eng = get_engine(use_ai=use_ai)
            result = eng.run(transactions)
        except Exception as engine_error:
            # Engine failed - provide helpful error message with schema info
            error_msg = str(engine_error)
            
            # Add schema compliance context if data was imported
            if import_metadata:
                compliance = import_metadata.get('compliance_score', 0)
                missing = import_metadata.get('missing_fields', [])[:5]
                warnings = import_metadata.get('warnings', [])
                
                error_msg = f"Pipeline execution failed: {error_msg}\n\n"
                error_msg += f"DATA FORMAT ISSUE DETECTED:\n"
                error_msg += f"Schema Compliance: {compliance:.0f}%\n"
                
                if missing:
                    error_msg += f"Missing required fields: {', '.join(missing)}\n"
                
                if compliance < 80:
                    error_msg += "\nYour data does not follow the required VISA transaction format.\n"
                    error_msg += "Please ensure your CSV/JSON includes these required fields:\n"
                    error_msg += "- transaction.transaction_id, transaction.amount, transaction.currency\n"
                    error_msg += "- card.network, card.card_type\n"
                    error_msg += "- merchant.merchant_id, merchant.country\n"
                    error_msg += "- customer.customer_id\n"
                    error_msg += "- fraud.risk_score\n"
                
                for w in warnings:
                    error_msg += f"\nWarning: {w}"
            
            return jsonify({
                "success": False,
                "error": error_msg,
                "import_metadata": {
                    "source": import_metadata.get("source", "unknown") if import_metadata else "unknown",
                    "compliance_score": import_metadata.get("compliance_score", 0) if import_metadata else 100,
                    "is_standard_format": import_metadata.get("is_standard_format", True) if import_metadata else True,
                    "missing_fields": import_metadata.get("missing_fields", [])[:10] if import_metadata else [],
                    "warnings": import_metadata.get("warnings", []) if import_metadata else []
                } if import_metadata else None
            }), 400
        
        # Build response
        layer_timings = []
        for t in result.layer_timings:
            layer_timings.append({
                "layer_id": t.layer_id,
                "layer_name": t.layer_name,
                "duration_ms": round(t.duration_ms, 2),
                "status": t.status,
                "start_time": t.start_time.isoformat(),
                "end_time": t.end_time.isoformat()
            })
        
        # Get detailed layer results
        layer_details = {}
        for lid, res in eng.layer_results.items():
            layer_details[str(lid)] = {
                "layer_id": res.layer_id,
                "layer_name": res.layer_name,
                "status": res.status.value,
                "checks_performed": res.checks_performed,
                "checks_passed": res.checks_passed,
                "issues_count": len(res.issues) if res.issues else 0,
                "warnings_count": len(res.warnings) if res.warnings else 0,
                "details": sanitize_for_json(res.details),
                "can_continue": res.can_continue
            }
        
        # Add import warnings to decision report if applicable
        decision_report = result.decision_report or ""
        if import_metadata and import_metadata.get("warnings"):
            warning_header = "\n\n--- DATA IMPORT NOTICE ---\n"
            warning_header += f"Schema Compliance: {import_metadata.get('compliance_score', 0):.0f}%\n"
            for warning in import_metadata.get("warnings", []):
                warning_header += f"Warning: {warning}\n"
            warning_header += "\nFor best results, use the standard VISA transaction format.\n"
            warning_header += "See documentation for required fields and structure.\n"
            warning_header += "-" * 30 + "\n\n"
            decision_report = warning_header + decision_report
        
        response = {
            "success": result.success,
            "batch_id": result.batch_id,
            "execution_id": result.execution_id,
            "total_records": result.total_records,
            "safe_count": result.safe_count,
            "review_count": result.review_count,
            "escalate_count": result.escalate_count,
            "rejected_count": result.rejected_count,
            "average_dqs": round(result.average_dqs, 2),
            "quality_rate": round(result.quality_rate, 2),
            "total_duration_ms": round(result.total_duration_ms, 2),
            "layer_timings": layer_timings,
            "layer_details": layer_details,
            "decision_report": decision_report,
            "execution_report": result.execution_report,
            "errors": result.errors
        }
        
        # Add import metadata if present
        if import_metadata:
            response["import_metadata"] = {
                "source": import_metadata.get("source", "unknown"),
                "compliance_score": import_metadata.get("compliance_score", 100),
                "is_standard_format": import_metadata.get("is_standard_format", True),
                "quality_penalty": import_metadata.get("quality_penalty", 0),
                "warnings": import_metadata.get("warnings", []),
                "missing_fields": import_metadata.get("missing_fields", [])[:10]
            }
        
        return jsonify(response)
        
    except Exception as e:
        import traceback
        return jsonify({
            "success": False, 
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@app.route('/api/layers')
def get_layers():
    """Get information about all layers."""
    layers = [
        {"id": 1, "name": "Input Contract", "phase": 1, "type": "Deterministic", "description": "Schema validation and manifest verification"},
        {"id": 2, "name": "Input Validation", "phase": 1, "type": "Deterministic", "description": "Parse and validate JSON transactions"},
        {"id": 3, "name": "Feature Extraction", "phase": 2, "type": "Deterministic", "description": "Extract 35+ quality dimensions"},
        {"id": 4.1, "name": "Structural Integrity", "phase": 3, "type": "Deterministic", "description": "Validate required fields and data types"},
        {"id": 4.2, "name": "Field Compliance", "phase": 3, "type": "Deterministic", "description": "Calculate Data Quality Score (DQS)"},
        {"id": 4.3, "name": "Semantic Validation", "phase": 3, "type": "Deterministic", "description": "Apply business rules and constraints"},
        {"id": 4.4, "name": "Anomaly Detection", "phase": 4, "type": "AI/ML", "description": "Isolation Forest anomaly detection"},
        {"id": 4.5, "name": "GenAI Summarization", "phase": 4, "type": "AI/GenAI", "description": "Generate quality summaries with Gemini"},
        {"id": 5, "name": "Output Contract", "phase": 5, "type": "Deterministic", "description": "Structure and validate outputs"},
        {"id": 6, "name": "Stability & Consistency", "phase": 5, "type": "Deterministic", "description": "Validate score distribution"},
        {"id": 7, "name": "Conflict Detection", "phase": 5, "type": "Deterministic", "description": "Identify signal conflicts"},
        {"id": 8, "name": "Confidence Band", "phase": 5, "type": "Deterministic", "description": "Calculate confidence levels"},
        {"id": 9, "name": "Decision Gate", "phase": 6, "type": "Deterministic", "description": "Make final action decisions"},
        {"id": 10, "name": "Responsibility Boundary", "phase": 6, "type": "Deterministic", "description": "Track decision ownership"},
        {"id": 11, "name": "Logging & Trace", "phase": 7, "type": "Deterministic", "description": "Complete audit trail"}
    ]
    
    phases = [
        {"id": 1, "name": "Foundation", "layers": [1, 2], "color": "#3498db"},
        {"id": 2, "name": "Feature Extraction", "layers": [3], "color": "#9b59b6"},
        {"id": 3, "name": "Deterministic Inference", "layers": [4.1, 4.2, 4.3], "color": "#2ecc71"},
        {"id": 4, "name": "AI Inference", "layers": [4.4, 4.5], "color": "#e74c3c"},
        {"id": 5, "name": "Output & Decision", "layers": [5, 6, 7, 8], "color": "#f39c12"},
        {"id": 6, "name": "Decision Gate", "layers": [9, 10], "color": "#1abc9c"},
        {"id": 7, "name": "Logging", "layers": [11], "color": "#34495e"}
    ]
    
    return jsonify({"layers": layers, "phases": phases})


@app.route('/api/schema')
def get_schema():
    """Get the expected VISA transaction schema."""
    schema = {
        "description": "VISA Transaction Schema for DQS Engine",
        "required_sections": [
            "transaction", "card", "merchant", "customer", 
            "authentication", "fraud", "network", "compliance", 
            "settlement", "business_metadata"
        ],
        "required_fields": {
            "transaction": ["transaction_id", "amount", "currency", "timestamp"],
            "card": ["network", "card_type"],
            "merchant": ["merchant_id", "merchant_category_code", "country"],
            "customer": ["customer_id"],
            "fraud": ["risk_score"]
        },
        "example": {
            "transaction": {
                "transaction_id": "txn_00000001",
                "amount": 5000.00,
                "currency": "INR",
                "timestamp": "2026-01-05T10:30:00Z",
                "status": "approved"
            },
            "card": {
                "network": "VISA",
                "card_type": "credit"
            },
            "merchant": {
                "merchant_id": "MID_1234",
                "merchant_category_code": "5812",
                "country": "IN"
            },
            "customer": {
                "customer_id": "cust_1234"
            },
            "fraud": {
                "risk_score": 25
            }
        }
    }
    return jsonify(schema)


if __name__ == '__main__':
    print("=" * 60)
    print("  DQS ENGINE - Enterprise Data Quality Platform")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60)
    app.run(debug=True, port=5000)
