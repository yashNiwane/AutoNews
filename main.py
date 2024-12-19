from pytrends.request import TrendReq
import pandas as pd
import os
from datetime import datetime, timedelta
import schedule
import time

csv_file_path = "trending_searches_india.csv"
metadata_file_path = "file_metadata.txt"

def fetch_trends():
    try:
        pytrends = TrendReq()
        new_trends = pytrends.trending_searches(pn='india')[0].tolist()
        current_date = datetime.now().strftime("%Y-%m-%d")

        if os.path.exists(csv_file_path) and os.stat(csv_file_path).st_size > 0:
            trends_df = pd.read_csv(csv_file_path)
            historical_trends = trends_df["Historical Trends"].dropna().tolist()
            unique_new_trends = [trend for trend in new_trends if trend not in historical_trends]
            historical_trends.extend(unique_new_trends)
            updated_df = pd.DataFrame({
                "Current Trends": pd.Series(unique_new_trends),
                "Historical Trends": pd.Series(historical_trends)
            })
        else:
            updated_df = pd.DataFrame({
                "Current Trends": pd.Series(new_trends),
                "Historical Trends": pd.Series(new_trends)
            })

        updated_df.to_csv(csv_file_path, index=False)
        print(f"[{current_date}] Trends updated successfully. New unique trends: {len(updated_df['Current Trends'].dropna())}")
    except Exception as e:
        print(f"Error fetching trends: {e}")

def clean_old_file():
    try:
        if os.path.exists(metadata_file_path):
            with open(metadata_file_path, "r") as meta_file:
                creation_date = datetime.strptime(meta_file.read().strip(), "%Y-%m-%d")
            if datetime.now() > creation_date + timedelta(days=5):
                for file in [csv_file_path, metadata_file_path]:
                    if os.path.exists(file): os.remove(file)
                with open(metadata_file_path, "w") as meta_file:
                    meta_file.write(datetime.now().strftime("%Y-%m-%d"))
                fetch_trends()
        else:
            with open(metadata_file_path, "w") as meta_file:
                meta_file.write(datetime.now().strftime("%Y-%m-%d"))
    except Exception as e:
        print(f"Error cleaning files: {e}")

def scheduler():
    print("Scheduler initialized. Tasks:")
    print("- Fetch trends: Daily at 1:00 AM")
    print("- Clean old files: Daily at 12:00 AM")
    schedule.every().day.at("01:00").do(fetch_trends)
    schedule.every().day.at("00:00").do(clean_old_file)
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            print("\nScheduler stopped.")
            break
        except Exception as e:
            print(f"Scheduler error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    clean_old_file()
    fetch_trends()
    scheduler()
  
