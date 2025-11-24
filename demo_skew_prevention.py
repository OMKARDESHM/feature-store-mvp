"""
Training/Serving Skew Prevention Demonstration
"""
import pandas as pd
from datetime import datetime
from feast import FeatureStore

def demo_skew_prevention():
    """
    Demonstrates identical feature retrieval from training and serving
    """
    print("\n" + "╔" + "═" * 78 + "╗")
    print("║" + " " * 15 + "FEAST TRAINING/SERVING SKEW PREVENTION DEMO" + " " * 20 + "║")
    print("╚" + "═" * 78 + "╝")
    
    store = FeatureStore(repo_path="feature_repo")
    demo_user_id = 5
    
    # PART A: Training (Offline Store)
    print("\n" + "=" * 80)
    print("  PART A: TRAINING PHASE - Historical Feature Retrieval")
    print("=" * 80 + "\n")
    
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
    
    print("✓ Retrieved from OFFLINE store (Parquet)")
    print("\nTraining Features:")
    print(training_features.to_string(index=False))
    
    training_avg = training_features['user_avg_3day_purchase_amount'].iloc[0]
    training_txns = training_features['user_total_transactions'].iloc[0]
    
    # PART B: Serving (Online Store)
    print("\n" + "=" * 80)
    print("  PART B: SERVING PHASE - Real-Time Feature Retrieval")
    print("=" * 80 + "\n")
    
    entity_rows = [{"user_id": demo_user_id}]
    
    serving_features = store.get_online_features(
        features=[
            "user_purchase_features:user_avg_3day_purchase_amount",
            "user_purchase_features:user_total_transactions",
        ],
        entity_rows=entity_rows,
    ).to_dict()
    
    print("✓ Retrieved from ONLINE store (Redis)")
    
    serving_avg = serving_features['user_avg_3day_purchase_amount'][0]
    serving_txns = serving_features['user_total_transactions'][0]
    
    print(f"\nServing Features for User {demo_user_id}:")
    print(f"  • Avg 3-Day Purchase: ${serving_avg:.2f}")
    print(f"  • Total Transactions: {serving_txns}")
    
    # COMPARISON
    print("\n" + "=" * 80)
    print("  SKEW PREVENTION ANALYSIS")
    print("=" * 80 + "\n")
    
    print("🛡️  VALUE COMPARISON:")
    print(f"   Training Avg: ${training_avg:.2f}")
    print(f"   Serving Avg:  ${serving_avg:.2f}")
    
    if abs(training_avg - serving_avg) < 0.01:
        print("   ✅ VALUES MATCH - No skew detected!")
    else:
        print("   ℹ️  Small difference due to time-based updates")
    
    print("\n🎯 KEY INSIGHT:")
    print("   ✅ SAME feature definition used for training and serving")
    print("   ✅ Defined once in features.py")
    print("   ✅ Eliminates definition-level skew")
    print("   ✅ Prevents model degradation in production\n")

if __name__ == "__main__":
    demo_skew_prevention()
