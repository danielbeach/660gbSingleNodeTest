from datetime import datetime
import polars as pl

t1 = datetime.now()

lf = pl.scan_iceberg(
    source="s3://confessions-of-a-data-guy/TBtest/catalog/wendigo/social_media_eats_your_brains/metadata/00101-36917495-22c6-40e7-b331-a8148402e897.metadata.json",
    use_metadata_statistics=False,
    reader_override="pyiceberg",     
)

lf = (
    lf.select(["user_id", "post_type", "likes_count"])
      .with_columns(pl.col("likes_count").cast(pl.Float64, strict=False))
)

result = (
    lf.select(
        pl.len().alias("total_posts"),                            
        pl.col("user_id").n_unique().alias("unique_users"),
        pl.col("post_type").n_unique().alias("unique_post_types"),
        pl.col("likes_count").count().alias("posts_with_likes"),
        pl.col("likes_count").mean().alias("avg_likes"),
        pl.col("likes_count").max().alias("max_likes"),
        pl.col("likes_count").sum().alias("total_likes"),
    )
    .collect()
)

print("\n📊 Results:")
print(result)
print("\nDict:", result.row(0).as_dict())
print("\n⏱️ Duration:", datetime.now() - t1)