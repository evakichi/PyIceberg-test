import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from datetime import datetime

# curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet -o /tmp/yellow_tripdata_2023-01.parquet

dt_now = datetime.now()
dt_now_str = dt_now.strftime("%Y%m%d%H%M%S%f")

df = pq.read_table("/tmp/yellow_tripdata_2023-01.parquet")
catalog = load_catalog("default")
catalog.create_namespace("default"+dt_now_str)
table = catalog.create_table(
    "default.taxi_dataset"+dt_now_str,
    schema=df.schema,
)
table.append(df)
length = len(table.scan().to_arrow())
print(length)
