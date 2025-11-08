import duckdb
from boringcatalog import BoringCatalog
from datetime import datetime


t1 = datetime.now()
catalog = BoringCatalog(
    name="TBtest",
    uri="s3://confessions-of-a-data-guy/TBtest/catalog/catalog.json",
)

namespace = "wendigo"
table_name = "social_media_eats_your_brains"
identifier = f"{namespace}.{table_name}"

table = catalog.load_table(identifier)

con = duckdb.connect(database=":memory:")
con.execute("SET unsafe_enable_version_guessing = true;")
con.execute("PRAGMA memory_limit='28GB'")        
con.execute("PRAGMA threads=13")
con.execute("SET temp_directory = '/tmp/duckdb_swap';")

con.execute("""
	INSTALL httpfs;
    LOAD httpfs;
    INSTALL iceberg;
    LOAD iceberg;
""")


table_path = table.location()
print(f"📂 Table location: {table_path}")

con.execute(f"""
    CREATE OR REPLACE VIEW social_media_eats_your_brains AS
    SELECT * FROM iceberg_scan('{table_path}')
""")


query = """
SELECT 
    COUNT(*) AS total_posts,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT post_type) AS unique_post_types,
    COUNT(likes_count) AS posts_with_likes,
    AVG(likes_count) AS avg_likes,
    MAX(likes_count) AS max_likes,
    SUM(likes_count) AS total_likes
FROM iceberg_scan('{table_path}');
"""

print("🧠 Running analytics query...")
result_df = con.execute(query).df()


print("\n📊 Results:")
print(result_df.to_markdown(index=False))
t2 = datetime.now()
print(t2-t1)