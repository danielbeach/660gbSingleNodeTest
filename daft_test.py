from datetime import datetime
import daft
from pyiceberg.table import StaticTable

t0 = datetime.now()

# 1) Load Iceberg table directly from metadata (no catalog needed)
table = StaticTable.from_metadata(
    "s3://confessions-of-a-data-guy/TBtest/catalog/wendigo/social_media_eats_your_brains/metadata/00101-36917495-22c6-40e7-b331-a8148402e897.metadata.json"
)

# 2) Create a Daft DataFrame
social_media_eats_your_brains = daft.read_iceberg(table)  # <- the variable name is the SQL table name

# 3) Run your SQL via daft.sql(); it can reference the DF by variable name
query = """
WITH t AS (
  SELECT
    user_id,
    post_type,
    CAST(likes_count AS BIGINT) AS likes_count
  FROM social_media_eats_your_brains
)
SELECT 
    COUNT(*)                           AS total_posts,
    COUNT(DISTINCT user_id)            AS unique_users,
    COUNT(DISTINCT post_type)          AS unique_post_types,
    COUNT(likes_count)                 AS posts_with_likes,
    AVG(likes_count)                   AS avg_likes,
    MAX(likes_count)                   AS max_likes,
    SUM(likes_count)                   AS total_likes
FROM t;
"""

result = daft.sql(query).collect()

print("\n📊 Results:")
print(result.to_pydict())
print("⏱️ Duration:", datetime.now() - t0)
