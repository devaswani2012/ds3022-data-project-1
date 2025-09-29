import duckdb
import logging
import pandas as pd

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

def transform_data():
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        flush_logs()

        # Drop existing transform tables if they exist
        con.execute("DROP TABLE IF EXISTS yellow_tripdata_transform;")
        con.execute("DROP TABLE IF EXISTS green_tripdata_transform;")
        logger.info("Dropped existing transform tables if they existed")
        flush_logs()

        
        con.execute("""
        CREATE TABLE IF NOT EXISTS yellow_tripdata_transform (VendorID INTEGER, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE, co2_kg DOUBLE, avg_mph DOUBLE, trip_hours DOUBLE, trip_day INTEGER, trip_week INTEGER, trip_month INTEGER);
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS green_tripdata_transform (VendorID INTEGER, lpep_pickup_datetime TIMESTAMP, lpep_dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE, co2_kg DOUBLE, avg_mph DOUBLE, trip_hours DOUBLE, trip_day INTEGER, trip_week INTEGER, trip_month INTEGER);
        """)
        logger.info("Initialized transform tables")
        flush_logs()

        df = pd.read_csv('data/vehicle_emissions.csv')
        yellow_co2_per_mile = df[df['vehicle_type'] == 'yellow_taxi']['co2_grams_per_mile'].values[0]
        green_co2_per_mile = df[df['vehicle_type'] == 'green_taxi']['co2_grams_per_mile'].values[0]

        for year in range(2015, 2025):
            logger.info(f"Transforming data for year {year}")
            flush_logs()

                        
            con.execute(f"""
                INSERT INTO yellow_tripdata_transform
                SELECT *,
                       (trip_distance * {yellow_co2_per_mile}) AS trip_co2_kgs,
                        (trip_distance)/NULLIF((EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600), 0) AS avg_mph,
                        (EXTRACT(HOUR FROM tpep_pickup_datetime)) AS hour_of_day,
                        (strftime('%A', tpep_pickup_datetime)) AS day_of_week,
                        (EXTRACT(WEEK FROM tpep_pickup_datetime)) AS week_of_year,
                        (strftime('%m', tpep_pickup_datetime)) AS month_of_year
                FROM yellow_tripdata_clean
                WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {year};
            """)
            logger.info(f"Transformed yellow trip data for year {year}")
            flush_logs()

            logger.info(f"Transforming green trip data for year {year}")
            flush_logs()

            # Calculate CO2 emissions, average speed, trip duration in hours, trip day, trip week, trip month
            con.execute(f"""
                INSERT INTO green_tripdata_transform
                SELECT *,
                       (trip_distance * {green_co2_per_mile}) AS trip_co2_kgs,
                        (trip_distance)/NULLIF((EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600), 0) AS avg_mph,
                        (EXTRACT(HOUR FROM lpep_pickup_datetime)) AS hour_of_day,
                        (strftime('%A', lpep_pickup_datetime)) AS day_of_week,
                        (EXTRACT(WEEK FROM lpep_pickup_datetime)) AS week_of_year,
                        (strftime('%m', lpep_pickup_datetime)) AS month_of_year
                FROM green_tripdata_clean
                WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = {year};
            """)
            logger.info(f"Transformed green trip data for year {year}")
            flush_logs()
        
            

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        flush_logs()

if __name__ == "__main__":
    transform_data()
