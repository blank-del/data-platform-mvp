import duckdb
from pathlib import Path

# Connect or create DuckDB database file
con = duckdb.connect("mvp.duckdb")

# Create raw schema if it doesn't exist
con.sql("CREATE SCHEMA IF NOT EXISTS raw")

# File patterns - use absolute paths from root folder
root_dir = Path(__file__).parent.parent  # go up from duck_db/ to root
created_pattern = str(root_dir / "OUT_DIR" /"data_avro" / "orders_created_*.avro")
completed_pattern = str(root_dir / "OUT_DIR" /"data_avro" / "orders_completed_*.avro")

# Create tables if not exist, else append
def load_table(schema: str, table_name: str, pattern: str):
    full_name = f"{schema}.{table_name}"
    # Check if table exists
    tables = [t[0] for t in con.sql("SHOW TABLES FROM mvp.raw").fetchall()]
    if table_name not in tables:
        print(f"Creating table {table_name} from {pattern}")
        con.sql(f"CREATE TABLE {full_name} AS SELECT * FROM read_avro('{pattern}')")
    else:
        print(f"Appending into {table_name} from {pattern}")
        con.sql(f"INSERT INTO {full_name} SELECT * FROM read_avro('{pattern}')")

# Load both
load_table("raw", "order_created", created_pattern)
load_table("raw", "order_completed", completed_pattern)

# Verification
print("\nTables in DuckDB:")
print(con.sql("SHOW TABLES"))

print("\nSchema for raw.order_created:")
print(con.sql("DESCRIBE raw.order_created"))

print("\nSchema for raw.order_completed:")
print(con.sql("DESCRIBE raw.order_completed"))

created_count = con.sql("SELECT count(*) FROM raw.order_created").fetchone()[0]
completed_count = con.sql("SELECT count(*) FROM raw.order_completed").fetchone()[0]

print(f"\nRow count → raw.order_created: {created_count}")
print(f"Row count → raw.order_completed: {completed_count}")

# Sample preview
print("\nLatest 5 created orders:")
print(con.sql("SELECT * FROM raw.order_created ORDER BY created_at DESC LIMIT 5"))

print("\nLatest 5 completed orders:")
print(con.sql("SELECT * FROM raw.order_completed ORDER BY completed_at DESC LIMIT 5"))

con.close()
print("\nLoad complete.")