from datetime import datetime
import daft
from daft import col
from pyiceberg.table import StaticTable

t0 = datetime.now()

# Load Iceberg table directly from metadata JSON
table = StaticTable.from_metadata(
    "s3://confessions-of-a-data-guy/TBtest/catalog/wendigo/social_media_eats_your_brains/metadata/00101-36917495-22c6-40e7-b331-a8148402e897.metadata.json"
)

# Read it with Daft
df = daft.read_iceberg(table)

# Select and cast
df = (
    df.select("user_id", "post_type", "likes_count")
      .with_column("likes_count", col("likes_count").cast(daft.DataType.int64()))
)

# --- Aggregations ---
# Daft uses .agg() with expressions, not daft.count()
result = df.agg(
    [
        col("user_id").count().alias("unique_users"),
        col("post_type").count().alias("unique_post_types"),
        col("likes_count").count().alias("posts_with_likes"),
        col("likes_count").mean().alias("avg_likes"),
        col("likes_count").max().alias("max_likes"),
        col("likes_count").sum().alias("total_likes"),
    ]
).collect()

# For total rows, use len(df) or a separate count:
total_rows = df.count_rows()
print(f"Total posts: {total_rows}")

print("\n📊 Aggregates:")
print(result.to_pydict())
print("⏱️ Duration:", datetime.now() - t0)
