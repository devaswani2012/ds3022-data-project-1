import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log',
    filemode='a',
    force=True
)
logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def flush_logs():
    for handler in logger.handlers:
        handler.flush()

def load_parquet_files():

    con = None  

    try:
        # Connect to loc    al DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        flush_logs()

        # Create consolidated tables if not exists
        con.execute("DROP TABLE IF EXISTS yellow_tripdata;")
        con.execute("""
        CREATE TABLE IF NOT EXISTS yellow_tripdata (VendorID INTEGER, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE);
        """)

        con.execute("DROP TABLE IF EXISTS green_tripdata;")
        con.execute("""
        CREATE TABLE IF NOT EXISTS green_tripdata (VendorID INTEGER, lpep_pickup_datetime TIMESTAMP, lpep_dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE);
        """)
        logger.info("Initialized consolidated tables")
        flush_logs()

        # Loop over 10 years and all the months
        for year in range(2015, 2025):
            for month in range(1, 13):
                month_str = f"{month:02d}"
                
                # Load yellow trip data
                yellow_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month_str}.parquet"
                
                try:
                    time.sleep(60) 
                    con.execute(f"""
                    INSERT INTO yellow_tripdata SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance FROM read_parquet('{yellow_url}');
                    """)
                    logger.info(f"Loaded yellow trip data for {year}-{month_str}")
                except Exception as e:
                    logger.warning(f"Failed to load yellow trip data for {year}-{month_str}: {e}")
                flush_logs()    

                # Load green trip data
                green_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month_str}.parquet"
                
                try: 
                    time.sleep(60)  
                    con.execute(f"""
                    INSERT INTO green_tripdata SELECT VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count, trip_distance FROM read_parquet('{green_url}');
                    """)
                    logger.info(f"Loaded green trip data for {year}-{month_str}")
                except Exception as e:
                    logger.warning(f"Failed to load green trip data for {year}-{month_str}: {e}")
                flush_logs()
        
        # Basic Descriptive Statitics
        yellow_mean1 = con.execute("SELECT AVG(trip_distance) FROM yellow_tripdata").fetchone()[0]
        green_mean1 = con.execute("SELECT AVG(trip_distance) FROM green_tripdata").fetchone()[0]
        logger.info(f"Average trip distance for yellow tripdata: {yellow_mean1}")
        logger.info(f"Average trip distance for green tripdata: {green_mean1}")
        flush_logs()

        yellow_mean2 = con.execute("SELECT AVG(passenger_count) FROM yellow_tripdata").fetchone()[0]
        green_mean2 = con.execute("SELECT AVG(passenger_count) FROM green_tripdata").fetchone()[0]
        logger.info(f"Average passenger count for yellow tripdata: {yellow_mean2}")
        logger.info(f"Average passenger count for green tripdata: {green_mean2}")
        flush_logs()


    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        flush_logs()

if __name__ == "__main__":
    load_parquet_files()
