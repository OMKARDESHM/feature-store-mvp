"""
Feature Computation Script
Computes user purchase features from transaction data
"""
import pandas as pd
from datetime import timedelta

def compute_user_features():
    """
    Compute user_avg_3day_purchase_amount feature
    """
    print("=" * 70)
    print("FEATURE COMPUTATION PIPELINE")
    print("=" * 70)
    
    # Read transaction data
    print("\n1. Reading transaction data...")
    df = pd.read_csv('user_transactions.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f"   ✓ Loaded {len(df)} transactions")
    
    # Sort by user and timestamp
    df = df.sort_values(['user_id', 'timestamp'])
    
    # Compute 3-day rolling average per user
    print("\n2. Computing 3-day rolling average features...")
    user_features = []
    
    for user_id in df['user_id'].unique():
        user_df = df[df['user_id'] == user_id].copy()
        
        # For each transaction, compute average of last 3 days
        for idx, row in user_df.iterrows():
            current_time = row['timestamp']
            lookback_start = current_time - timedelta(days=3)
            
            # Get transactions in 3-day window
            window_df = user_df[
                (user_df['timestamp'] >= lookback_start) & 
                (user_df['timestamp'] <= current_time)
            ]
            
            avg_3day = window_df['purchase_amount'].mean()
            total_txns = len(window_df)
            
            user_features.append({
                'user_id': user_id,
                'event_timestamp': current_time,
                'user_avg_3day_purchase_amount': round(avg_3day, 2),
                'user_total_transactions': total_txns
            })
    
    features_df = pd.DataFrame(user_features)
    
    # Get latest feature value per user
    latest_features = features_df.sort_values('event_timestamp').groupby('user_id').tail(1)
    
    print(f"   ✓ Computed features for {latest_features['user_id'].nunique()} users")
    
    # Save to parquet
    print("\n3. Saving to offline store (Parquet)...")
    output_path = 'data/user_features.parquet'
    features_df.to_parquet(output_path, index=False)
    print(f"   ✓ Saved to {output_path}")
    
    print("\n4. Sample features:")
    print(latest_features.head(10).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("FEATURE COMPUTATION COMPLETE")
    print("=" * 70)
    
    return features_df

if __name__ == "__main__":
    compute_user_features()
