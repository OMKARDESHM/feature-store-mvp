"""
Feature Materialization Script - Enhanced Version
Synchronizes offline features to online Redis store with detailed debugging
"""
import sys
import os
from datetime import datetime, timedelta
from feast import FeatureStore
import pandas as pd

def check_prerequisites():
    """Check all prerequisites before materialization"""
    print("="*70)
    print("PREREQUISITES CHECK")
    print("="*70 + "\n")
    
    issues = []
    
    # Check 1: Redis connection
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, socket_connect_timeout=5)
        r.ping()
        print("✓ Redis: Connected and responsive")
    except Exception as e:
        print(f"✗ Redis: {str(e)}")
        issues.append(f"Redis not accessible: {str(e)}")
        print("\n  Fix: Run these commands:")
        print("    redis-server --daemonize yes --port 6379")
        print("    redis-cli ping")
    
    # Check 2: Offline data exists
    parquet_path = "data/user_features.parquet"
    if os.path.exists(parquet_path):
        size = os.path.getsize(parquet_path)
        print(f"✓ Offline Store: {parquet_path} ({size} bytes)")
        
        # Check if file has data
        try:
            df = pd.read_parquet(parquet_path)
            print(f"  - Rows: {len(df)}")
            print(f"  - Users: {df['user_id'].nunique()}")
            print(f"  - Date range: {df['event_timestamp'].min()} to {df['event_timestamp'].max()}")
        except Exception as e:
            print(f"  ⚠ Warning: Could not read parquet: {e}")
    else:
        print(f"✗ Offline Store: {parquet_path} not found")
        issues.append("Offline features not computed")
        print("\n  Fix: Run this command:")
        print("    python compute_features.py")
    
    # Check 3: Feast registry
    registry_path = "feature_repo/data/registry.db"
    if os.path.exists(registry_path):
        print(f"✓ Feast Registry: {registry_path}")
    else:
        print(f"✗ Feast Registry: {registry_path} not found")
        issues.append("Feast not initialized")
        print("\n  Fix: Run these commands:")
        print("    cd feature_repo && feast apply && cd ..")
    
    # Check 4: Feature store config
    config_path = "feature_repo/feature_store.yaml"
    if os.path.exists(config_path):
        print(f"✓ Feast Config: {config_path}")
    else:
        print(f"✗ Feast Config: {config_path} not found")
        issues.append("Feast configuration missing")
    
    print()
    
    if issues:
        print("="*70)
        print("❌ PREREQUISITES NOT MET")
        print("="*70)
        print("\nIssues found:")
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")
        print("\nPlease fix the issues above and try again.")
        return False
    
    print("="*70)
    print("✅ ALL PREREQUISITES MET")
    print("="*70 + "\n")
    return True

def materialize_features_to_online_store():
    """
    Materialize latest feature values from offline store to Redis online store
    """
    print("\n" + "="*70)
    print("FEATURE MATERIALIZATION: Offline → Online Store")
    print("="*70 + "\n")
    
    # Check prerequisites first
    if not check_prerequisites():
        sys.exit(1)
    
    # Initialize Feast Feature Store
    try:
        print("Initializing Feast Feature Store...")
        store = FeatureStore(repo_path="feature_repo")
        print("✓ Feature Store initialized\n")
    except Exception as e:
        print(f"✗ Failed to initialize Feature Store: {str(e)}")
        print("\nTroubleshooting:")
        print("  1. Verify you're in the project root directory")
        print("  2. Check that feature_repo/feature_store.yaml exists")
        print("  3. Run: cd feature_repo && feast apply && cd ..")
        sys.exit(1)
    
    # List available feature views
    try:
        feature_views = store.list_feature_views()
        print("Available Feature Views:")
        for fv in feature_views:
            print(f"  - {fv.name}")
            print(f"    Entities: {[e.name for e in fv.entities]}")
            print(f"    Features: {[f.name for f in fv.schema]}")
            print(f"    TTL: {fv.ttl}")
        print()
    except Exception as e:
        print(f"⚠ Warning: Could not list feature views: {str(e)}\n")
    
    # Define time range for materialization
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"📅 Materialization Time Range:")
    print(f"   Start: {start_date}")
    print(f"   End:   {end_date}")
    print()
    
    try:
        print("🔄 Materializing features to Redis...")
        print("   (This may take 10-30 seconds)")
        print()
        
        # Perform materialization
        store.materialize(
            start_date=start_date,
            end_date=end_date,
        )
        
        print("✓ Materialization completed successfully!\n")
        
        # Verify materialization
        print("="*70)
        print("VERIFICATION: Checking Online Store")
        print("="*70 + "\n")
        
        # Try to retrieve a sample feature
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379)
            
            # Count keys
            keys = r.keys("*")
            key_count = len(keys)
            
            print(f"Redis Statistics:")
            print(f"  - Total keys: {key_count}")
            
            if key_count > 0:
                print(f"  - Sample keys:")
                for key in list(keys)[:5]:
                    print(f"    • {key.decode('utf-8')}")
                
                # Try to retrieve features for a test user
                print("\nTest Retrieval (User ID = 1):")
                try:
                    test_features = store.get_online_features(
                        features=[
                            "user_purchase_features:user_avg_3day_purchase_amount",
                            "user_purchase_features:user_total_transactions",
                        ],
                        entity_rows=[{"user_id": 1}],
                    ).to_dict()
                    
                    avg = test_features['user_avg_3day_purchase_amount'][0]
                    txns = test_features['user_total_transactions'][0]
                    
                    if avg is not None:
                        print(f"  ✓ Avg 3-Day Purchase: ${avg:.2f}")
                        print(f"  ✓ Total Transactions: {txns}")
                        print("\n✅ Online features are accessible!")
                    else:
                        print("  ⚠ Features retrieved but values are None")
                        print("    This may indicate timing issues with materialization")
                except Exception as e:
                    print(f"  ✗ Could not retrieve features: {str(e)}")
            else:
                print("\n⚠ WARNING: Redis is empty after materialization!")
                print("\nPossible causes:")
                print("  1. Offline data timestamp range doesn't match materialization range")
                print("  2. Feature definitions don't match data schema")
                print("  3. Redis connection was lost during materialization")
                print("\nDebugging steps:")
                print("  1. Check offline data: python -c 'import pandas as pd; df = pd.read_parquet(\"data/user_features.parquet\"); print(df.head())'")
                print("  2. Verify timestamps: python -c 'import pandas as pd; df = pd.read_parquet(\"data/user_features.parquet\"); print(df[\"event_timestamp\"].min(), df[\"event_timestamp\"].max())'")
                print("  3. Re-run with wider date range")
                
        except Exception as e:
            print(f"Could not verify Redis contents: {str(e)}")
        
        print("\n" + "="*70)
        print("MATERIALIZATION PROCESS COMPLETE")
        print("="*70 + "\n")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error during materialization: {str(e)}")
        print("\nDetailed error information:")
        import traceback
        traceback.print_exc()
        
        print("\nCommon issues and solutions:")
        print("  1. Redis not running:")
        print("     → redis-server --daemonize yes")
        print("  2. Wrong timestamp range:")
        print("     → Check your data timestamps match materialization range")
        print("  3. Schema mismatch:")
        print("     → Verify features.py matches your parquet schema")
        
        return False

if __name__ == "__main__":
    print("\n🚀 Starting Feature Materialization Process...\n")
    
    success = materialize_features_to_online_store()
    
    if success:
        print("\n✅ Feature materialization completed successfully!")
        print("   Features are now ready for real-time inference\n")
        print("Next steps:")
        print("  1. Run demo: python demo_skew_prevention.py")
        print("  2. Query features: python query_features.py")
        print()
        sys.exit(0)
    else:
        print("\n❌ Feature materialization failed!")
        print("   Please review the errors above and try again.\n")
        sys.exit(1)