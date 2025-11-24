"""
Synthetic Data Generator for E-commerce Feature Store MVP
Generates user transaction data for testing feature store pipeline
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_user_transactions(num_rows=100, output_file='user_transactions.csv'):
    """
    Generate synthetic e-commerce transaction data
    
    Args:
        num_rows: Number of transaction records to generate
        output_file: Output CSV filename
    """
    np.random.seed(42)
    
    # Generate data
    user_ids = np.random.choice(range(1, 21), size=num_rows)  # 20 unique users
    product_ids = np.random.choice(range(100, 150), size=num_rows)  # 50 products
    
    # Generate timestamps over last 30 days
    base_date = datetime.now() - timedelta(days=30)
    timestamps = [
        base_date + timedelta(
            days=np.random.randint(0, 30),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        ) for _ in range(num_rows)
    ]
    
    # Generate purchase amounts (realistic distribution)
    purchase_amounts = np.random.gamma(shape=2, scale=25, size=num_rows).round(2)
    
    # Create DataFrame
    df = pd.DataFrame({
        'user_id': user_ids,
        'product_id': product_ids,
        'timestamp': timestamps,
        'purchase_amount': purchase_amounts
    })
    
    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"✓ Generated {num_rows} transactions for {df['user_id'].nunique()} users")
    print(f"✓ Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"✓ Saved to: {output_file}")
    print("\nSample data:")
    print(df.head(10))
    
    return df

if __name__ == "__main__":
    generate_user_transactions(num_rows=100)
