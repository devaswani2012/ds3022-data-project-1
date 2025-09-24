import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

DB_FILE = "emissions.duckdb"  # same DB as load.py

def clean_taxi_table(con, color):
    raw_table = f"{color}_trips_2024"
    clean_table = f"{color}_trips_2024_clean"

    # Determine correct pickup/dropoff columns
    if color == "yellow":
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
    elif color == "green":
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
    else:
        raise ValueError("Unknown taxi color")

    # Create cleaned table
    con.execute(f"""
        CREATE OR REPLACE TABLE {clean_table} AS
        SELECT *
        FROM (
            SELECT DISTINCT *
            FROM {raw_table}
        ) t
        WHERE passenger_count > 0
          AND trip_distance > 0
          AND trip_distance <= 100
          AND (CAST({dropoff_col} AS TIMESTAMP) - CAST({pickup_col} AS TIMESTAMP)) <= INTERVAL '1 DAY';
    """)
    logger.info(f"{clean_table} created from {raw_table}")

    # Row counts before and after cleaning
    before = con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    after = con.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
    removed = before - after
    logger.info(f"{color} trips removed: {removed:,}")
    print(f"{color} trips removed: {removed:,}")

    # Verification queries
    checks = {
        "0 passengers": f"SELECT COUNT(*) FROM {clean_table} WHERE passenger_count = 0",
        "0 miles": f"SELECT COUNT(*) FROM {clean_table} WHERE trip_distance = 0",
        ">100 miles": f"SELECT COUNT(*) FROM {clean_table} WHERE trip_distance > 100",
        ">1 day": f"SELECT COUNT(*) FROM {clean_table} WHERE (CAST({dropoff_col} AS TIMESTAMP) - CAST({pickup_col} AS TIMESTAMP)) > INTERVAL '1 DAY'"
    }

    for check, query in checks.items():
        count = con.execute(query).fetchone()[0]
        logger.info(f"{clean_table} check ({check}): {count}")
        print(f"{clean_table} check ({check}): {count}")

def main():
    con = duckdb.connect(DB_FILE)

    print("Cleaning Yellow taxi data...")
    clean_taxi_table(con, "yellow")

    print("Cleaning Green taxi data...")
    clean_taxi_table(con, "green")

    con.close()
    logger.info("DuckDB connection closed")

if __name__ == "__main__":
    main()
