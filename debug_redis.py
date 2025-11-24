"""
Redis Debugging and Feature Materialization Troubleshooter
"""
import sys
import os
from datetime import datetime, timedelta
import pandas as pd

def print_header(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")

def check_redis_connection():
    """Check if Redis is accessible"""
    print_header("STEP 1: Redis Connection Check")
    
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        response = r.ping()
        print(f"✅ Redis PING: {response}")
        
        info = r.info()
        print(f"✅ Redis version: {info.get('redis_version', 'unknown')}")
        print(f"✅ Used memory: {info.get('used_memory_human', 'unknown')}")
        
        return r
    except Exception as e:
        print(f"❌ Redis connection failed: {str(e)}")
        sys.exit(1)

def check_redis_keys(redis_client):
    """Check what keys exist in Redis"""
    print_header("STEP 2: Redis Keys Inspection")
    
    all_keys = redis_client.keys('*')
    
    if not all_keys:
        print("❌ Redis is EMPTY - No keys found!")
        print("\n🔍 Features not materialized yet.")
        return False
    else:
        print(f"✅ Found {len(all_keys)} keys in Redis")
        return True

def check_parquet_file():
    """Check if offline store exists"""
    print_header("STEP 3: Offline Store Check")
    
    parquet_path = 'data/user_features.parquet'
    
    if not os.path.exists(parquet_path):
        print(f"❌ Parquet file NOT FOUND: {parquet_path}")
        return None
    
    try:
        df = pd.read_parquet(parquet_path)
        print(f"✅ Parquet file exists")
        print(f"✅ Rows: {len(df)}")
        print(f"✅ Users: {df['user_id'].nunique()}")
        
        if 'event_timestamp' in df.columns:
            min_ts = df['event_timestamp'].min()
            max_ts = df['event_timestamp'].max()
            print(f"✅ Timestamp range: {min_ts} to {max_ts}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error reading Parquet: {str(e)}")
        return None

def check_feast_registry():
    """Check Feast registry"""
    print_header("STEP 4: Feast Registry Check")
    
    try:
        from feast import FeatureStore
        
        store = FeatureStore(repo_path="feature_repo")
        feature_views = store.list_feature_views()
        
        if not feature_views:
            print("❌ No feature views registered!")
            return None
        
        print(f"✅ Found {len(feature_views)} feature view(s)")
        
        for fv in feature_views:
            print(f"  📊 {fv.name}")
        
        return store
        
    except Exception as e:
        print(f"❌ Feast error: {str(e)}")
        return None

def manual_materialization(store, df):
    """Perform materialization"""
    print_header("STEP 5: Materialization")
    
    if store is None or df is None:
        print("❌ Cannot materialize - missing store or data")
        return False
    
    try:
        min_ts = pd.Timestamp(df['event_timestamp'].min())
        max_ts = pd.Timestamp(df['event_timestamp'].max())
        
        start_date = min_ts - timedelta(days=1)
        end_date = max_ts + timedelta(days=1)
        
        print(f"📅 Time range: {start_date} to {end_date}")
        print(f"\n🔄 Materializing features (30-60 seconds)...")
        
        store.materialize(
            start_date=start_date,
            end_date=end_date,
        )
        
        print("\n✅ Materialization completed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Materialization failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def verify_materialization(redis_client, store):
    """Verify features in Redis"""
    print_header("STEP 6: Verification")
    
    all_keys = redis_client.keys('*')
    
    if not all_keys:
        print("❌ Redis is STILL EMPTY")
        return False
    
    print(f"✅ Redis now has {len(all_keys)} keys")
    
    try:
        features = store.get_online_features(
            features=[
                "user_purchase_features:user_avg_3day_purchase_amount",
                "user_purchase_features:user_total_transactions",
            ],
            entity_rows=[{"user_id": 5}],
        ).to_dict()
        
        avg = features['user_avg_3day_purchase_amount'][0]
        txns = features['user_total_transactions'][0]
        
        print(f"\n✅ Feature retrieval SUCCESS!")
        print(f"   User 5 - Avg Purchase: ${avg:.2f}, Transactions: {txns}")
        return True
        
    except Exception as e:
        print(f"\n❌ Feature retrieval failed: {str(e)}")
        return False

def main():
    print("\n╔══════════════════════════════════════════════════════════════════╗")
    print("║          REDIS & FEAST MATERIALIZATION DEBUGGER                 ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    
    redis_client = check_redis_connection()
    has_keys = check_redis_keys(redis_client)
    df = check_parquet_file()
    store = check_feast_registry()
    
    if not has_keys:
        print_header("ATTEMPTING MATERIALIZATION")
        if manual_materialization(store, df):
            verify_materialization(redis_client, store)
    else:
        verify_materialization(redis_client, store)
    
    print_header("SUMMARY")
    
    redis_keys = redis_client.keys('*')
    
    if redis_keys:
        print("✅ SUCCESS! Redis has feature data")
        print(f"   Total keys: {len(redis_keys)}")
        print("\n✅ Next: python demo_skew_prevention.py")
    else:
        print("❌ FAILED - Redis is still empty")
        print("\nManual fix:")
        print("   python materialize_to_online.py")
    
    print("\n" + "=" * 70 + "\n")

if __name__ == "__main__":
    main()
