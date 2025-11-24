from feast import FeatureStore
from datetime import datetime, timedelta

print("Starting materialization...")

store = FeatureStore(repo_path="feature_repo")

# Materialize last 60 days to be safe
end = datetime.now()
start = end - timedelta(days=60)

print(f"Time range: {start} to {end}")

store.materialize(start_date=start, end_date=end)

print("Done!")

# Verify
import redis
r = redis.Redis(host='localhost', port=6379)
keys = r.keys("*")
print(f"Redis has {len(keys)} keys")
if len(keys) > 0:
    print("Sample keys:", keys[:3])
