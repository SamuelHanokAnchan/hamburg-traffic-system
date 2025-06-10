# batch_processor.py
import pandas as pd
import snowflake.connector
import json
import schedule
import time
from datetime import datetime, timedelta

# Load Snowflake config
with open("snowflake_config.json") as f:
    SNOWFLAKE_CONFIG = json.load(f)

def connect_snowflake():
    """Create Snowflake connection"""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def hourly_aggregation():
    """Run every hour - aggregate last hour's data"""
    print(f"🕐 Running hourly aggregation at {datetime.now()}")
    
    conn = connect_snowflake()
    cs = conn.cursor()
    
    try:
        # Aggregate last hour's data
        query = """
        INSERT INTO HOURLY_SUMMARY (HOUR, TOTAL_VEHICLES, AVG_SPEED, TOTAL_RECORDS)
        SELECT 
            DATE_TRUNC('HOUR', TIMESTAMP) as HOUR,
            SUM(VEHICLE_COUNT) as TOTAL_VEHICLES,
            AVG(SPEED) as AVG_SPEED,
            COUNT(*) as TOTAL_RECORDS
        FROM LIVE_STREAM 
        WHERE TIMESTAMP >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
        AND TIMESTAMP < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP())
        GROUP BY DATE_TRUNC('HOUR', TIMESTAMP)
        """
        
        cs.execute(query)
        print(f"✅ Hourly aggregation completed")
        
    except Exception as e:
        print(f"❌ Hourly aggregation failed: {e}")
    finally:
        cs.close()
        conn.close()

def daily_summary():
    """Run daily - create daily summaries and cleanup"""
    print(f"📅 Running daily summary at {datetime.now()}")
    
    conn = connect_snowflake()
    cs = conn.cursor()
    
    try:
        # Create daily summary
        query = """
        INSERT INTO DAILY_SUMMARY (DATE, TOTAL_VEHICLES, AVG_SPEED, PEAK_HOUR, RECORDS_PROCESSED)
        SELECT 
            DATE(HOUR) as DATE,
            SUM(TOTAL_VEHICLES) as TOTAL_VEHICLES,
            AVG(AVG_SPEED) as AVG_SPEED,
            HOUR(MAX_BY(HOUR, TOTAL_VEHICLES)) as PEAK_HOUR,
            SUM(TOTAL_RECORDS) as RECORDS_PROCESSED
        FROM HOURLY_SUMMARY 
        WHERE DATE(HOUR) = CURRENT_DATE() - 1
        GROUP BY DATE(HOUR)
        """
        
        cs.execute(query)
        
        # Optional: Cleanup old raw data (keep last 7 days)
        cleanup_query = """
        DELETE FROM LIVE_STREAM 
        WHERE TIMESTAMP < DATEADD(DAY, -7, CURRENT_TIMESTAMP())
        """
        
        cs.execute(cleanup_query)
        print(f"✅ Daily summary and cleanup completed")
        
    except Exception as e:
        print(f"❌ Daily processing failed: {e}")
    finally:
        cs.close()
        conn.close()

def retrain_model():
    """Run weekly - retrain ML model with fresh data"""
    print(f"🧠 Retraining model at {datetime.now()}")
    
    try:
        # Import your existing training script
        import train_traffic_model
        train_traffic_model.main()
        print(f"✅ Model retrained successfully")
        
    except Exception as e:
        print(f"❌ Model retraining failed: {e}")

def data_quality_check():
    """Run every 6 hours - check data quality"""
    print(f"🔍 Running data quality check at {datetime.now()}")
    
    conn = connect_snowflake()
    cs = conn.cursor()
    
    try:
        # Check for missing data
        missing_data_query = """
        SELECT COUNT(*) as missing_hours
        FROM (
            SELECT GENERATE_SERIES(
                DATEADD(HOUR, -24, DATE_TRUNC('HOUR', CURRENT_TIMESTAMP())),
                DATE_TRUNC('HOUR', CURRENT_TIMESTAMP()),
                INTERVAL '1 HOUR'
            ) as expected_hour
        ) expected
        LEFT JOIN HOURLY_SUMMARY h ON expected.expected_hour = h.HOUR
        WHERE h.HOUR IS NULL
        """
        
        cs.execute(missing_data_query)
        result = cs.fetchone()
        missing_hours = result[0] if result else 0
        
        if missing_hours > 0:
            print(f"⚠️ Warning: {missing_hours} hours of missing data detected")
        else:
            print(f"✅ Data quality check passed")
            
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
    finally:
        cs.close()
        conn.close()

def setup_tables():
    """Create batch processing tables if they don't exist"""
    conn = connect_snowflake()
    cs = conn.cursor()
    
    try:
        # Create HOURLY_SUMMARY table
        cs.execute("""
        CREATE TABLE IF NOT EXISTS HOURLY_SUMMARY (
            HOUR TIMESTAMP PRIMARY KEY,
            TOTAL_VEHICLES INTEGER,
            AVG_SPEED FLOAT,
            TOTAL_RECORDS INTEGER,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        # Create DAILY_SUMMARY table
        cs.execute("""
        CREATE TABLE IF NOT EXISTS DAILY_SUMMARY (
            DATE DATE PRIMARY KEY,
            TOTAL_VEHICLES INTEGER,
            AVG_SPEED FLOAT,
            PEAK_HOUR INTEGER,
            RECORDS_PROCESSED INTEGER,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        print("✅ Batch processing tables created/verified")
        
    except Exception as e:
        print(f"❌ Table setup failed: {e}")
    finally:
        cs.close()
        conn.close()

# Schedule the jobs
def schedule_jobs():
    """Schedule all batch processing jobs"""
    print("📋 Setting up batch processing schedule...")
    
    # Hourly jobs
    schedule.every().hour.at(":05").do(hourly_aggregation)
    schedule.every(6).hours.do(data_quality_check)
    
    # Daily jobs
    schedule.every().day.at("02:00").do(daily_summary)
    
    # Weekly jobs
    schedule.every().sunday.at("03:00").do(retrain_model)
    
    print("✅ Batch processing schedule configured:")
    print("   - Hourly aggregation: Every hour at :05")
    print("   - Data quality check: Every 6 hours")
    print("   - Daily summary: Every day at 2:00 AM")
    print("   - Model retraining: Every Sunday at 3:00 AM")

def run_batch_processor():
    """Main function to run the batch processor"""
    print("🚀 Starting Hamburg Traffic Batch Processor...")
    
    # Setup tables first
    setup_tables()
    
    # Setup schedule
    schedule_jobs()
    
    # Keep running
    print("⏰ Batch processor is running. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n🛑 Batch processor stopped by user")

if __name__ == "__main__":
    run_batch_processor()