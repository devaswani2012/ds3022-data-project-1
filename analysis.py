import duckdb
import logging
import matplotlib.pyplot as plt
import seaborn as sns

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

def analyze_data():
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        flush_logs()

        # largest carbon producing trip
        result1 = con.execute("""
            SELECT 'yellow' AS service_type, VendorID, tpep_pickup_datetime AS pickup_datetime, tpep_dropoff_datetime AS dropoff_datetime, trip_distance, trip_co2_kgs
            FROM yellow_tripdata_transform
            ORDER BY trip_co2_kgs DESC
            LIMIT 1
        """).fetchdf()
        logger.info("Largest carbon producing trips for yellow taxi:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT 'green' AS service_type, VendorID, lpep_pickup_datetime AS pickup_datetime, lpep_dropoff_datetime AS dropoff_datetime, trip_distance, trip_co2_kgs
            FROM green_tripdata_transform
            ORDER BY trip_co2_kgs DESC
            LIMIT 1
        """).fetchdf()
        logger.info("Largest carbon producing trips for green taxi:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        # on average most carbon heavy and light of the day
        result1 = con.execute("""
            SELECT service_type, hour_of_day, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'yellow' AS service_type, hour_of_day, trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type, hour_of_day
            ORDER BY avg_co2_kg DESC
            LIMIT 1;
            """).fetchdf()
        logger.info("Most Carbon Heavy by yellow taxi by hour of day:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result1 = con.execute("""
            SELECT service_type, hour_of_day, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'yellow' AS service_type, hour_of_day, trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type, hour_of_day
            ORDER BY avg_co2_kg ASC
            LIMIT 1;
            """).fetchdf()
        logger.info("Least Carbon Heavy by yellow taxi by hour of day:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT service_type, hour_of_day, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'green' AS service_type, hour_of_day, trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type, hour_of_day
            ORDER BY avg_co2_kg DESC
            LIMIT 1;
            """).fetchdf()
        logger.info("Average CO2 emissions by green taxi by hour of day:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT service_type, hour_of_day, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'green' AS service_type, hour_of_day, trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type, hour_of_day
            ORDER BY avg_co2_kg ASC
            LIMIT 1;
            """).fetchdf()
        logger.info("Average CO2 emissions by green taxi by hour of day:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()        

        # on average most carbon heavy and light of the week
        result1 = con.execute("""
            SELECT service_type,  day_of_week , AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'yellow' AS service_type,  day_of_week , trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type,  day_of_week 
            ORDER BY avg_co2_kg DESC
            LIMIT 1;
        """).fetchdf()
        logger.info("Most Carbon Heavy by day of week for yellow taxi:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result1 = con.execute("""
            SELECT service_type,  day_of_week , AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'yellow' AS service_type,  day_of_week , trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type,  day_of_week 
            ORDER BY avg_co2_kg ASC
            LIMIT 1;
        """).fetchdf()
        logger.info("Least Carbon Heavy by day of week for yellow taxi:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT service_type,  day_of_week , AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'green' AS service_type,  day_of_week , trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type,  day_of_week 
            ORDER BY avg_co2_kg DESC
            LIMIT 1;
        """).fetchdf()
        logger.info("Most Carbon Heavy by day of week for green taxi:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT service_type,  day_of_week , AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'green' AS service_type,  day_of_week , trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type,  day_of_week 
            ORDER BY avg_co2_kg ASC
            LIMIT 1;
        """).fetchdf()
        logger.info("Least Carbon Heavy by day of week for green taxi:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        # on average most carbon heavy and light of the month
        result1 = con.execute("""
            SELECT service_type, week_of_year, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'yellow' AS service_type, week_of_year, trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type, week_of_year
            ORDER BY avg_co2_kg DESC
            LIMIT 1;
        """).fetchdf()
        logger.info("Most Carbon Heavy by week of year for yellow taxi:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result1 = con.execute("""
            SELECT service_type, week_of_year, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'yellow' AS service_type, week_of_year, trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type, week_of_year
            ORDER BY avg_co2_kg ASC
            LIMIT 1;
        """).fetchdf()
        logger.info("Least Carbon Heavy by week of year for yellow taxi:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()         

        result2 = con.execute("""
            SELECT service_type, week_of_year, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'green' AS service_type, week_of_year, trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type, week_of_year
            ORDER BY avg_co2_kg DESC
            LIMIT 1;
        """).fetchdf()
        logger.info("Most Carbon Heavy by week of year for green taxi:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT service_type, week_of_year, AVG(trip_co2_kgs) AS avg_co2_kg
            FROM (
                SELECT 'green' AS service_type, week_of_year, trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type, week_of_year
            ORDER BY avg_co2_kg ASC
            LIMIT 1;
        """).fetchdf()
        logger.info("Least Carbon Heavy by week of year for green taxi:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        # total emissions by year
        result1 = con.execute("""
            SELECT service_type, month_of_year, AVG(trip_co2_kgs) AS total_co2_kg
            FROM (
                SELECT 'yellow' AS service_type, month_of_year, trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type, month_of_year
            ORDER BY total_co2_kg DESC
            LIMIT 1;
        """).fetchdf()
        logger.info("Most Carbon Heavy by year for yellow taxi:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result1 = con.execute("""
            SELECT service_type, month_of_year, AVG(trip_co2_kgs) AS total_co2_kg
            FROM (
                SELECT 'yellow' AS service_type, month_of_year, trip_co2_kgs
                FROM yellow_tripdata_transform
            )
            GROUP BY service_type, month_of_year
            ORDER BY total_co2_kg ASC
            LIMIT 1;
        """).fetchdf()
        logger.info("Least Carbon Heavy by year for yellow taxi:")
        logger.info(result1.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT service_type, month_of_year, AVG(trip_co2_kgs) AS total_co2_kg
            FROM (
                SELECT 'green' AS service_type, month_of_year, trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type, month_of_year
            ORDER BY total_co2_kg DESC
            LIMIT 1;
        """).fetchdf()
        logger.info("Most Carbon Heavy by year for green taxi:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        result2 = con.execute("""
            SELECT service_type, month_of_year, AVG(trip_co2_kgs) AS total_co2_kg
            FROM (
                SELECT 'green' AS service_type, month_of_year, trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type, month_of_year
            ORDER BY total_co2_kg ASC
            LIMIT 1;
        """).fetchdf()
        logger.info("Least Carbon Heavy by year for green taxi:")
        logger.info(result2.to_dict(orient='records'))
        flush_logs()

        # Visualizations

        # Time series CO2

	        # Visualizations

        # Time series CO2
        df = con.execute("""
            SELECT service_type, trip_year, SUM(trip_co2_kgs) AS total_co2_kg
            FROM (
                SELECT 'yellow' AS service_type, EXTRACT(YEAR FROM tpep_pickup_datetime) AS trip_year, trip_co2_kgs
                FROM yellow_tripdata_transform
                UNION ALL
                SELECT 'green' AS service_type, EXTRACT(YEAR FROM lpep_pickup_datetime) AS trip_year, trip_co2_kgs
                FROM green_tripdata_transform
            )
            GROUP BY service_type, trip_year
            ORDER BY trip_year;
        """).fetchdf()

        plt.figure(figsize=(10, 6))
        for service_type, group in df.groupby("service_type"):
            plt.plot(
                group["trip_year"].to_numpy(),
                group["total_co2_kg"].to_numpy(),
                marker="o",
                label=service_type
            )

        plt.title("Total CO2 Emissions by Year")
        plt.xlabel("Year")
        plt.ylabel("Total CO2 Emissions (kg)")
        plt.legend(title="Service Type")
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.savefig("total_co2_emissions_by_year.png")
        plt.close()
        logger.info("Saved visualization: total_co2_emissions_by_year.png")
        flush_logs()
        
    except Exception as e:
        logger.error(f"Error during data analysis: {e}")
        flush_logs()

if __name__ == "__main__":
    analyze_data()
