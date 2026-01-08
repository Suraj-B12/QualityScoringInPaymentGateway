import sys
import logging
# Ensure src is in path
sys.path.append('.')

from app import live_generator, eng
from src.live_data_generator import LiveDataGenerator

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format='%(message)s')

def debug_one_transaction():
    print("Generating transaction...")
    # Increase anomaly rate to ensure we catch the issue
    live_generator.set_anomaly_rate(0.5)
    
    transaction = live_generator.generate_transaction()
    print(f"Generated Transaction ID: {transaction['transaction']['transaction_id']}")
    
    print("\nRunning DQS Engine...")
    try:
        pipeline_result = eng.run([transaction])
        
        print("\n=== PIPELINE RESULT ===")
        print(f"Success: {pipeline_result.success}")
        print(f"Safe Count: {pipeline_result.safe_count}")
        print(f"Escalate Count: {pipeline_result.escalate_count}")
        
        print("\n=== DECISION REPORT ===")
        print(pipeline_result.decision_report)
        
        # Also print Layer 4.3 Semantic Results if possible
        # Accessing internal layer results from pipeline_result requires accessing private attributes or using exposed headers
        # But decision report should contain the primary reason.
        
    except Exception as e:
        print(f"CRASH: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_one_transaction()
