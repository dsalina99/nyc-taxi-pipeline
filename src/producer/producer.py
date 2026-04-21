# src/producer/producer.py
import boto3, pandas as pd, json, time, uuid
from datetime import datetime

df = pd.read_parquet('data/yellow_tripdata_2023-01.parquet')
kinesis = boto3.client('kinesis', region_name='us-east-1')

for _, row in df.iterrows():
    record = row.to_dict()
    # Convert timestamps to strings for JSON serialization
    for k, v in record.items():
        if hasattr(v, 'isoformat'):
            record[k] = v.isoformat()
    record['ride_id'] = str(uuid.uuid4())
    record['ingestion_timestamp'] = datetime.utcnow().isoformat()
    # Partition by pickup location for load distribution
    partition_key = str(int(record.get('PULocationID', 1)))

    kinesis.put_record(
        StreamName='nyc-taxi-stream',
        Data=json.dumps(record),
        PartitionKey=partition_key
    )
    time.sleep(0.05)  # 50ms = ~20 events/sec
    print(f"Sent ride {record['ride_id'][:8]}...")