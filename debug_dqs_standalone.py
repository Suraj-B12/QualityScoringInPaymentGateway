
import sys
import os
import logging

# Add current directory to path
sys.path.append(os.getcwd())

from src.dqs_engine import DQSEngine
from src.live_data_generator import LiveDataGenerator

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format='%(message)s')

def debug_one_transaction():
    print("Initializing components...")
    eng = DQSEngine()
    live_generator = LiveDataGenerator(anomaly_rate=0.5)

    print("Generating transaction...")
    transaction = live_generator.generate_transaction()
    print(f"Generated Transaction ID: {transaction['transaction']['transaction_id']}")
    
    print("\nRunning DQS Engine...")
    try:
        pipeline_result = eng.run([transaction])
        
        print("\n=== PIPELINE RESULT ===")
        print(f"Success: {pipeline_result.success}")
        print(f"Safe Count: {pipeline_result.safe_count}")
        print(f"Escalate Count: {pipeline_result.escalate_count}")
        print(f"DQS Score: {pipeline_result.average_dqs}")
        
        print("\n=== DECISION REPORT ===")
        print(pipeline_result.decision_report)
        
    except Exception as e:
        print(f"CRASH: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_one_transaction()
