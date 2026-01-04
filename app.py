"""
Flask API Server for DQS Engine Frontend

Provides REST endpoints to interact with the Data Quality Scoring Engine.
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
        "version": "1.0.0",
        "layers": 15,
        "phases": 7
    })


@app.route('/api/generate', methods=['POST'])
def generate_data():
    """Generate sample VISA transactions."""
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
        
        # Return just first 5 as preview
        preview = transactions[:5] if isinstance(transactions, list) else [transactions]
        
        return jsonify({
            "success": True,
            "count": n_transactions,
            "anomaly_rate": anomaly_rate,
            "preview": preview,
            "message": f"Generated {n_transactions} transactions"
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/run', methods=['POST'])
def run_pipeline():
    """Run the complete DQS pipeline."""
    try:
        data = request.get_json() or {}
        
        # Check for custom data first
        custom_data = data.get('custom_data')
        use_ai = data.get('use_ai', False)
        
        if custom_data:
            # Use custom data provided by user
            transactions = custom_data if isinstance(custom_data, list) else [custom_data]
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
        
        # Run engine
        eng = get_engine(use_ai=use_ai)
        result = eng.run(transactions)
        
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
        
        return jsonify({
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
            "decision_report": result.decision_report,
            "execution_report": result.execution_report,
            "errors": result.errors
        })
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


if __name__ == '__main__':
    print("=" * 60)
    print("  DQS ENGINE - Web Interface")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60)
    app.run(debug=True, port=5000)
