import duckdb
from datetime import datetime

t1 = datetime.now()
# Initialize a DuckDB connection
con = duckdb.connect()

AWS_KEY = 'xxxxx'
AWS_SECRET = 'xxxxxxx'

con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL delta;  LOAD delta;")

# Provide S3 credentials in a way the Delta reader will definitely see
con.execute(f"""
CREATE OR REPLACE SECRET s3_creds (
  TYPE S3,
  KEY_ID '{AWS_KEY}',
  SECRET '{AWS_SECRET}',
  REGION 'us-east-1'
);
""")

# Point to your Delta Lake table on S3
table_path = "s3://confessions-of-a-data-guy/BigTest/social_media_rots_brains"

agg_df = con.execute(f"""
    SELECT 
        DATE(created_at) AS post_date,
        COUNT(*) AS num_posts
    FROM delta_scan('{table_path}')
    GROUP BY post_date
    ORDER BY post_date
""").fetch_df()

# Optional: save results locally for inspection
agg_df.to_csv("posts_per_day.csv", index=False)
t2 = datetime.now()
print(t2-t1)
