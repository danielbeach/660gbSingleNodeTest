import polars as pl
from datetime import datetime
import os

t1 = datetime.now()

table_path = "s3://confessions-of-a-data-guy/BigTest/social_media_rots_brains"

os.environ["AWS_ACCESS_KEY_ID"] = "xxxxxxxx"
os.environ["AWS_SECRET_ACCESS_KEY"] = "xxxxxxxxxxxxxxx"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

lazy_df = pl.scan_delta(table_path)

agg_lazy = (
    lazy_df
    .with_columns(pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias("created_at_ts"))
    .with_columns(pl.col("created_at_ts").dt.date().alias("post_date"))
    .group_by("post_date")
    .agg(pl.count().alias("num_posts"))
    .sort("post_date")
)

agg_lazy.sink_csv("posts_per_day.csv")

t2 = datetime.now()
print(f"Elapsed time: {t2 - t1}")
