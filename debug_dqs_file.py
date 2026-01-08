
import sys
import os
import logging
from src.dqs_engine import DQSEngine
from src.live_data_generator import LiveDataGenerator

sys.path.append(os.getcwd())

def debug_one_transaction():
    with open("debug_output.txt", "w") as f:
        f.write("Initializing components...\n")
        eng = DQSEngine()
        # Set anomaly to low first to see a SAFE transaction?
        # Or high to see ESCALATE. User said "all as escalate".
        live_generator = LiveDataGenerator(anomaly_rate=0.5)

        f.write("Generating transaction...\n")
        transaction = live_generator.generate_transaction()
        f.write(f"Generated Transaction ID: {transaction['transaction']['transaction_id']}\n")
        
        f.write("\nRunning DQS Engine...\n")
        try:
            pipeline_result = eng.run([transaction])
            
            f.write("\n=== PIPELINE RESULT ===\n")
            f.write(f"Success: {pipeline_result.success}\n")
            f.write(f"Safe Count: {pipeline_result.safe_count}\n")
            f.write(f"Escalate Count: {pipeline_result.escalate_count}\n")
            f.write(f"DQS Score: {pipeline_result.average_dqs}\n")
            
            f.write("\n=== DECISION REPORT ===\n")
            f.write(pipeline_result.decision_report)
            f.write("\n")
            
        except Exception as e:
            f.write(f"CRASH: {e}\n")
            import traceback
            traceback.print_exc(file=f)

if __name__ == "__main__":
    with open("debug_output.txt", "w") as f:
        f.write("Initializing components...\n")
        eng = DQSEngine()
        # High anomaly rate to catch issues
        live_generator = LiveDataGenerator(anomaly_rate=0.7)
        
        found_escalate = False
        for i in range(500):
            transaction = live_generator.generate_transaction()
            res = eng.run([transaction])
            
            if res.escalate_count > 0:
                f.write(f"\nFOUND ESCALATE at iteration {i}\n")
                f.write(f"Txn ID: {transaction['transaction']['transaction_id']}\n")
                f.write(f"DQS: {res.average_dqs}\n")
                f.write("=== REPORT ===\n")
                f.write(res.decision_report)
                found_escalate = True
                break
        
        if not found_escalate:
            f.write("No ESCALATE found in 20 iterations.\n")
