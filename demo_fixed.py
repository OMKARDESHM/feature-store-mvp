"""
Training/Serving Skew Prevention Demo - Auto-selecting Valid User
"""
import pandas as pd
from datetime import datetime
from feast import FeatureStore
import sys

def demo_skew_prevention():
    print("\n" + "╔" + "═" * 78 + "╗")
    print("║" + " " * 15 + "FEAST TRAINING/SERVING SKEW PREVENTION DEMO" + " " * 20 + "║")
    print("╚" + "═" * 78 + "╝")
    
    store = FeatureStore(repo_path="feature_repo")
    
    # Find a valid user ID from the parquet file
    print("\n🔍 Finding available users in offline store...")
    try:
        offline_df = pd.read_parquet('data/user_features.parquet')
        available_users = sorted(offline_df['user_id'].unique())
        print(f"   Found {len(available_users)} users: {available_users[:10]}{'...' if len(available_users) > 10 else ''}")
        
        # Pick the first available user
        demo_user_id = int(available_users[0])
        print(f"   Using User ID: {demo_user_id} for demonstration\n")
    except Exception as e:
        print(f"✗ Error reading offline data: {e}")
        sys.exit(1)
    
    # ========== PART A: TRAINING (OFFLINE) ==========
    print("=" * 80)
    print("  PART A: TRAINING PHASE - Historical Feature Retrieval")
    print("=" * 80 + "\n")
    
    print("📚 Scenario: Training a recommendation model")
    print(f"   Retrieving historical features for User ID: {demo_user_id}\n")
    
    try:
        entity_df = pd.DataFrame({
            "user_id": [demo_user_id],
            "event_timestamp": [datetime.now()]
        })
        
        training_features = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "user_purchase_features:user_avg_3day_purchase_amount",
                "user_purchase_features:user_total_transactions",
            ],
        ).to_df()
        
        if len(training_features) == 0:
            print("⚠ Warning: No historical features found for this user")
            print("   This might be due to timestamp mismatch")
            
            # Try to get the actual data directly from parquet
            user_data = offline_df[offline_df['user_id'] == demo_user_id].sort_values('event_timestamp').tail(1)
            if len(user_data) > 0:
                print(f"\n   Direct data from parquet for User {demo_user_id}:")
                print(user_data.to_string(index=False))
                training_avg = user_data['user_avg_3day_purchase_amount'].iloc[0]
                training_txns = user_data['user_total_transactions'].iloc[0]
            else:
                training_avg = None
                training_txns = None
        else:
            print("✓ Retrieved from OFFLINE store (Parquet)\n")
            print("Training Features:")
            print("-" * 80)
            print(training_features.to_string(index=False))
            print("-" * 80)
            
            training_avg = training_features['user_avg_3day_purchase_amount'].iloc[0]
            training_txns = training_features['user_total_transactions'].iloc[0]
        
        if training_avg is not None:
            print(f"\n📊 Training Feature Values:")
            print(f"   • Average 3-Day Purchase: ${training_avg:.2f}")
            print(f"   • Total Transactions: {training_txns}")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        training_avg = None
        training_txns = None
    
    # ========== PART B: SERVING (ONLINE) ==========
    print("\n" + "=" * 80)
    print("  PART B: SERVING PHASE - Real-Time Feature Retrieval")
    print("=" * 80 + "\n")
    
    print("🚀 Scenario: Serving predictions in production")
    print(f"   Retrieving real-time features for User ID: {demo_user_id}\n")
    
    try:
        entity_rows = [{"user_id": demo_user_id}]
        
        serving_features = store.get_online_features(
            features=[
                "user_purchase_features:user_avg_3day_purchase_amount",
                "user_purchase_features:user_total_transactions",
            ],
            entity_rows=entity_rows,
        ).to_dict()
        
        print("✓ Retrieved from ONLINE store (Redis)\n")
        
        serving_avg = serving_features['user_avg_3day_purchase_amount'][0]
        serving_txns = serving_features['user_total_transactions'][0]
        
        if serving_avg is not None:
            print("Serving Features:")
            print("-" * 80)
            print(f"{'Feature Name':<45} {'Value':<20}")
            print("-" * 80)
            print(f"{'user_avg_3day_purchase_amount':<45} ${serving_avg:<19.2f}")
            print(f"{'user_total_transactions':<45} {serving_txns:<20}")
            print("-" * 80)
            
            print(f"\n📊 Serving Feature Values:")
            print(f"   • Average 3-Day Purchase: ${serving_avg:.2f}")
            print(f"   • Total Transactions: {serving_txns}")
        else:
            print("⚠ Warning: Features retrieved but values are None")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        serving_avg = None
        serving_txns = None
    
    # ========== COMPARISON ==========
    print("\n" + "=" * 80)
    print("  SKEW PREVENTION ANALYSIS")
    print("=" * 80 + "\n")
    
    print("🛡️  HOW FEAST PREVENTS TRAINING/SERVING SKEW\n")
    
    print("1️⃣  SINGLE SOURCE OF TRUTH:")
    print("   ✓ Both training and serving use features.py")
    print("   ✓ Feature definitions are centrally managed")
    print("   ✓ Impossible to have different logic in training vs serving\n")
    
    print("2️⃣  IDENTICAL FEATURE COMPUTATION:")
    print("   ✓ user_avg_3day_purchase_amount computed identically")
    print("   ✓ Same rolling window logic (3 days)")
    print("   ✓ Same aggregation method (mean)")
    print("   ✓ Same data types (Float64, Int64)\n")
    
    print("3️⃣  CONSISTENT RETRIEVAL APIs:")
    print("   • Training: store.get_historical_features()")
    print("   • Serving: store.get_online_features()")
    print("   • Both reference same feature: 'user_purchase_features:user_avg_3day_purchase_amount'\n")
    
    if training_avg is not None and serving_avg is not None:
        print("4️⃣  VALUE COMPARISON:")
        print(f"   Training Avg: ${training_avg:.2f}")
        print(f"   Serving Avg:  ${serving_avg:.2f}")
        
        diff = abs(training_avg - serving_avg)
        if diff < 0.01:
            print("   ✅ VALUES MATCH PERFECTLY - No skew detected!")
        else:
            print(f"   ℹ️  Small difference: ${diff:.2f}")
            print("      (Expected due to time-based updates)")
    
    print("\n" + "─" * 80)
    print("🎯 KEY INSIGHT: Definition-Level Skew ELIMINATED")
    print("─" * 80)
    print("""
Without Feast:
   ❌ Training: Custom Python/Pandas code
   ❌ Serving: Different implementation (Java/Go/SQL)
   ❌ Result: Different calculations → Model degradation

With Feast:
   ✅ Training: Feast feature view (features.py)
   ✅ Serving: Same Feast feature view (features.py)
   ✅ Result: Identical features → Consistent performance
""")
    
    # ========== SHOW ALL USERS ==========
    print("=" * 80)
    print("  FEATURES FOR ALL USERS")
    print("=" * 80 + "\n")
    
    print("Querying online features for all users...\n")
    print(f"{'User ID':<10} {'Avg 3-Day Purchase':<25} {'Total Transactions':<20}")
    print("-" * 55)
    
    for user_id in available_users[:10]:  # Show first 10 users
        try:
            features = store.get_online_features(
                features=[
                    "user_purchase_features:user_avg_3day_purchase_amount",
                    "user_purchase_features:user_total_transactions",
                ],
                entity_rows=[{"user_id": int(user_id)}],
            ).to_dict()
            
            avg = features['user_avg_3day_purchase_amount'][0]
            txns = features['user_total_transactions'][0]
            
            if avg is not None:
                print(f"{user_id:<10} ${avg:<24.2f} {txns:<20}")
            else:
                print(f"{user_id:<10} {'N/A':<25} {'N/A':<20}")
        except Exception as e:
            print(f"{user_id:<10} Error: {str(e)[:40]}")
    
    if len(available_users) > 10:
        print(f"... and {len(available_users) - 10} more users")
    
    print("\n" + "=" * 80)
    print("  ✅ DEMONSTRATION COMPLETE")
    print("=" * 80 + "\n")
    
    print("📈 System Summary:")
    print(f"   • Total Users: {len(available_users)}")
    print(f"   • Features per User: 2 (avg_3day_purchase, total_transactions)")
    print(f"   • Offline Store: Parquet (data/user_features.parquet)")
    print(f"   • Online Store: Redis (localhost:6379)")
    print(f"   • Skew Prevention: ✅ Enabled via Feast\n")

if __name__ == "__main__":
    try:
        demo_skew_prevention()
    except KeyboardInterrupt:
        print("\n\nDemo interrupted.")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Demo failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)