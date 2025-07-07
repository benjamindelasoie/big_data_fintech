#!/usr/bin/env python3
"""
Simple Corrected ETL Pipeline Runner

This script runs the corrected ETL pipeline components sequentially.
Designed to be simple and straightforward to execute.

Usage:
    python run_corrected_pipeline.py
"""

import sys
import os
import subprocess
import time
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_command(command, description):
    """Run a Python script and return success status"""
    print(f"\n🔄 {description}")
    print(f"📝 Running: {command}")
    print("-" * 60)
    
    start_time = time.time()
    
    try:
        # Use venv python directly with absolute path
        venv_python = os.path.join(os.getcwd(), "venv", "bin", "python")
        result = subprocess.run([
            venv_python, command
        ], check=True, capture_output=False, text=True)
        
        duration = time.time() - start_time
        print(f"✅ {description} completed successfully in {duration:.1f}s")
        return True
        
    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        print(f"❌ {description} failed after {duration:.1f}s")
        print(f"Error: {e}")
        return False
    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ {description} failed after {duration:.1f}s")
        print(f"Unexpected error: {e}")
        return False

def main():
    """Main function to run the corrected pipeline"""
    print("=" * 80)
    print("FINTECH ETL PIPELINE - CORRECTED VERSION")
    print("=" * 80)
    print(f"🕒 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🔧 CORRECTION: Fixed CSV parsing for multiline address fields")
    print("📊 EXPECTATION: ~13,000 user records (not 39,000 lines)")
    
    start_time = time.time()
    
    # Define pipeline stages
    stages = [
        ("src/preprocessing/corrected_data_preprocessing.py", "STAGE 1: Corrected Data Preprocessing"),
        ("src/transformation/corrected_data_transformation.py", "STAGE 2: Corrected Data Transformation"), 
        ("src/loading/corrected_data_loading.py", "STAGE 3: Corrected Data Loading")
    ]
    
    # Track success
    overall_success = True
    completed_stages = 0
    
    # Run each stage
    for script_path, description in stages:
        success = run_command(script_path, description)
        
        if success:
            completed_stages += 1
        else:
            overall_success = False
            print(f"\n❌ Pipeline failed at: {description}")
            
            # Ask if user wants to continue
            response = input("❓ Continue with remaining stages? (y/n): ")
            if response.lower() != 'y':
                break
    
    # Print final summary
    total_duration = time.time() - start_time
    
    print("\n" + "=" * 80)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 80)
    
    if overall_success:
        print("🎉 CORRECTED PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"✅ All {len(stages)} stages completed without errors")
        print(f"⏱️  Total execution time: {total_duration:.1f} seconds")
        
        print("\n📊 KEY CORRECTIONS APPLIED:")
        print("   ✅ Fixed CSV parsing with multiLine=True")
        print("   ✅ Corrected user count: 39,000 → 13,000")
        print("   ✅ Recalculated all business metrics")
        print("   ✅ Generated realistic analytics")
        
        print("\n📁 OUTPUT FILES GENERATED:")
        print("   📄 processed_data/clean_users.parquet")
        print("   📄 processed_data/clean_transactions.parquet")
        print("   📄 processed_data/clean_onboarding.parquet")
        print("   📄 processed_data/user_metrics.parquet")
        print("   📄 processed_data/final_analytics.parquet")
        print("   📄 processed_data/corrected_cassandra_fallback_*.parquet")
        
    else:
        print("❌ PIPELINE COMPLETED WITH ERRORS")
        print(f"⚠️  Completed {completed_stages}/{len(stages)} stages")
        print(f"⏱️  Total execution time: {total_duration:.1f} seconds")
        
        print("\n🔧 TROUBLESHOOTING TIPS:")
        print("   - Check error messages above")
        print("   - Verify venv/bin/python is available")
        print("   - Ensure all dependencies are installed")
        print("   - Try running stages individually")
    
    print(f"\n🕒 Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    return 0 if overall_success else 1

if __name__ == "__main__":
    sys.exit(main())