import os
import daft
from daft import col
import daft.functions as F
from datetime import datetime

t1 = datetime.now()

table_path = "s3://confessions-of-a-data-guy/BigTest/social_media_rots_brains"
os.environ["AWS_ACCESS_KEY_ID"] = "xxxxxxxx"
os.environ["AWS_SECRET_ACCESS_KEY"] = "xxxxxxxxxxxxxxx"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

df = daft.read_deltalake(table_path)

df = df.with_column(
    "created_at_ts",
    F.to_datetime(col("created_at"), "%Y-%m-%d %H:%M:%S")
)

agg = (
    df.with_column("post_date", F.date(col("created_at_ts")))
      .groupby("post_date")
      .count()                                # produces a 'count' column
      .with_column_renamed("count", "num_posts")
      .sort("post_date")
)

agg.repartition(1).write_csv("posts_per_day.csv")

t2 = datetime.now()
print(f"Elapsed: {t2 - t1}")
