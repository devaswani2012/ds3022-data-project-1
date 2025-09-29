import duckdb
import logging

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

def clean_data():
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        flush_logs()

        # Drop existing clean tables if they exist
        con.execute("DROP TABLE IF EXISTS yellow_tripdata_clean;")
        con.execute("DROP TABLE IF EXISTS green_tripdata_clean;")
        logger.info("Dropped existing clean tables if they existed")
        flush_logs()

        # Final Clean Table
        con.execute("""
        CREATE TABLE IF NOT EXISTS yellow_tripdata_clean AS SELECT * FROM yellow_tripdata WHERE 1 = 0;
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS green_tripdata_clean AS SELECT * FROM green_tripdata WHERE 1 = 0;
        """)
        logger.info("Initialized clean tables")
        flush_logs()

        for year in range(2015, 2025):
            logger.info(f"Cleaning yellow trip data for year {year}")
            flush_logs()

            con.execute(f"""
                INSERT INTO yellow_tripdata_clean
                SELECT DISTINCT * 
                FROM yellow_tripdata
                WHERE strftime(tpep_pickup_datetime, '%Y') = '{year}'
                  AND passenger_count > 0
                  AND trip_distance > 0
                  AND trip_distance <= 100
                  AND (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600) <= 24;
            """) # Remove duplicates, trip with 0 passenger, trips 0 miles in length, trips greater than 100 miles, trips greater than 24 hrs
            logger.info(f"Cleaned yellow trip data for year {year}")
            flush_logs()

            logger.info(f"Cleaning green trip data for year {year}")
            flush_logs()

            con.execute(f"""
                INSERT INTO green_tripdata_clean
                SELECT DISTINCT *
                FROM green_tripdata
                WHERE strftime(lpep_pickup_datetime, '%Y') = '{year}'
                  AND passenger_count > 0
                  AND trip_distance > 0
                  AND trip_distance <= 100
                  AND (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600) <= 24;
            """) # Remove duplicates, trip with 0 passenger, trips 0 miles in length, trips greater than 100 miles, trips greater than 24 hrs
            logger.info(f"Cleaned green trip data for year {year}")
            flush_logs()

    except Exception as e:
        logger.error(f"Error during data cleaning: {e}")
        flush_logs()

def verify_clean_data():
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=True)
        logger.info("Connected to DuckDB instance for verification")
        flush_logs()

        # Verify yellow tripdata clean table
        yellow_count = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean").fetchone()[0]
        logger.info(f"Yellow tripdata clean table has {yellow_count} records.")
        flush_logs()

        # Verify green tripdata clean table
        green_count = con.execute("SELECT COUNT(*) FROM green_tripdata_clean").fetchone()[0]
        logger.info(f"Green tripdata clean table has {green_count} records.")
        flush_logs()

        # Verify no 0 passengers
        yellow_zero_passengers = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean WHERE passenger_count = 0").fetchone()[0]
        green_zero_passengers = con.execute("SELECT COUNT(*) FROM green_tripdata_clean WHERE passenger_count = 0").fetchone()[0]
        logger.info(f"Yellow tripdata clean table has {yellow_zero_passengers} records with 0 passengers.")
        logger.info(f"Green tripdata clean table has {green_zero_passengers} records with 0 passengers.")
        flush_logs()

        # Verify no 0 miles
        yellow_zero_miles = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean WHERE trip_distance = 0").fetchone()[0]
        green_zero_miles = con.execute("SELECT COUNT(*) FROM green_tripdata_clean WHERE trip_distance = 0").fetchone()[0]
        logger.info(f"Yellow tripdata clean table has {yellow_zero_miles} records with 0 miles.")
        logger.info(f"Green tripdata clean table has {green_zero_miles} records with 0 miles.")
        flush_logs()

        #Verify no trips greater than 100 miles
        yellow_over_100_miles = con.execute("SELECT COUNT(*) FROM yellow_tripdata_clean WHERE trip_distance > 100").fetchone()[0]
        green_over_100_miles = con.execute("SELECT COUNT(*) FROM green_tripdata_clean WHERE trip_distance > 100").fetchone()[0]
        logger.info(f"Yellow tripdata clean table has {yellow_over_100_miles} records with over 100 miles.")
        logger.info(f"Green tripdata clean table has {green_over_100_miles} records with over 100 miles.")
        flush_logs()

        # Verify no trips greater than 24 hours
        yellow_over_24hrs = con.execute("""SELECT COUNT(*) FROM yellow_tripdata_clean 
            WHERE (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600) > 24""").fetchone()[0]
        green_over_24hrs = con.execute("""SELECT COUNT(*) FROM green_tripdata_clean 
            WHERE (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600) > 24""").fetchone()[0]
        logger.info(f"Yellow tripdata clean table has {yellow_over_24hrs} records over 24 hours.")
        logger.info(f"Green tripdata clean table has {green_over_24hrs} records over 24 hours.")
        flush_logs()

    except Exception as e:
        logger.error(f"Error during verification: {e}")
        flush_logs()
   
if __name__ == "__main__":
    clean_data()
    verify_clean_data()
    logger.info("Data cleaning and verification completed.")

