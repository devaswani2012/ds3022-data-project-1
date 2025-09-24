import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

DATA_DIR = "data"
EMISSIONS_FILE = os.path.join(DATA_DIR, "vehicle_emissions.csv")

def load_taxi_data(con, color):
    table_name = f"{color}_trips_2024"
    # first file creates table
    first_file = os.path.join(DATA_DIR, 
f"{color}_tripdata_2024-01.parquet")
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{first_file}');")
    logger.info(f"{table_name} created from {first_file}")

    # remaining months
    for month in range(2, 13):
        file_path = os.path.join(DATA_DIR, 
f"{color}_tripdata_2024-{month:02d}.parquet")
        if os.path.exists(file_path):
            con.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{file_path}');")
            logger.info(f"{file_path} inserted into {table_name}")
        else:
            logger.warning(f"{file_path} not found, skipping.")

def load_emissions(con):
    con.execute(f"CREATE OR REPLACE TABLE vehicle_emissions AS SELECT * FROM read_csv_auto('{EMISSIONS_FILE}');")
    logger.info("vehicle_emissions table created")

def load_parquet_files():
    con = None
    try:
        # Connect to local DuckDB
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Load taxi data
        logger.info("Loading Yellow taxi data...")
        load_taxi_data(con, "yellow")

        logger.info("Loading Green taxi data...")
        load_taxi_data(con, "green")

        # Load emissions lookup
        logger.info("Loading vehicle emissions data...")
        load_emissions(con)

        # Print row counts
        for table in ["yellow_trips_2024", "green_trips_2024", "vehicle_emissions"]:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"{table}: {count:,} rows")
            print(f"{table}: {count:,} rows")

        # Additional basic descriptive stats for Yellow and Green trips
        for table in ["yellow_trips_2024", "green_trips_2024"]:
            stats = con.execute(f"""
                SELECT 
                    SUM(trip_distance) AS total_distance,
                    SUM(fare_amount) AS total_fare,
                    AVG(trip_distance) AS avg_distance,
                    AVG(fare_amount) AS avg_fare
                FROM {table};
            """).fetchone()
            
            total_distance, total_fare, avg_distance, avg_fare = stats
            logger.info(f"{table} stats - total distance: {total_distance}, total fare: {total_fare}, "
                        f"avg distance: {avg_distance:.2f}, avg fare: {avg_fare:.2f}")
            print(f"{table} stats - total distance: {total_distance}, total fare: {total_fare}, "
                  f"avg distance: {avg_distance:.2f}, avg fare: {avg_fare:.2f}")

        logger.info("All data loaded successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed")

if __name__ == "__main__":
    load_parquet_files()

