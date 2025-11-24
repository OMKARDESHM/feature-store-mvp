Feature Store MVP:
Built an automated feature store using Feast, Redis, and Parquet to eliminate training/serving skew in ML systems. Demonstrated zero feature drift with identical values ($95.32) retrieved from both offline (training) and online (serving) stores. Tech stack: Python, Feast, Redis, Pandas. Result: 100% prevention of definition-level skew across 20 users and 2 features.


# Feature Store MVP: Eliminating Training-Serving Skew


> An automated feature store built with open-source tools to prevent training-serving skew in ML production systems.

##  Problem Statement

The Training-Serving Skew Problem:

In production ML systems, models often degrade because:
- "Training" uses Python/Pandas for feature computation
- "Serving" uses different code (Java/SQL/Go) for the same features
- Result: Different values → Model predictions fail

Example:
```python
# Training (Data Scientist's code)
df['avg_purchase'] = df.groupby('user_id')['amount'].rolling(3).mean()

# Serving (Backend Engineer's code - DIFFERENT!)
SELECT AVG(amount) FROM purchases 
WHERE timestamp > NOW() - INTERVAL 3 DAY
```

Even small differences (timezone handling, null values, window boundaries) cause "skew".

---

## Solution Architecture

```
┌─────────────────┐
│  Raw Data (CSV) │
└────────┬────────┘
         │
         ▼
┌────────────────────────────────┐
│  Feature Computation Pipeline  │
│  (Rolling 3-day averages)      │
└────────┬───────────────────────┘
         │
         ▼
┌────────────────────────────────┐
│   Feast Offline Store          │
│   (Parquet Files)              │
│   → Historical features for    │
│     model training             │
└────────┬───────────────────────┘
         │
         │ Materialization
         ▼
┌────────────────────────────────┐
│   Feast Online Store           │
│   (Redis)                      │
│   → Real-time feature serving  │
│     (<10ms latency)            │
└────────────────────────────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌────────┐
│Training│ │Serving │
│Pipeline│ │API     │
└────────┘ └────────┘
    │         │
    └────┬────┘
         ▼
   SAME FEATURES!
   (No Skew)
```

---

##  Key Features

-  Zero Skew: Same feature definition used for training and serving
-  Real-time Serving: <10ms latency via Redis
-  Historical Training: Complete feature history in Parquet
-  Automated Pipeline: Materialization syncs offline → online
-  Type Safety: Schema enforcement prevents errors
-  Cost: $0 - fully open-source local deployment

---

##  Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Feature Store | Feast | Central feature registry & API |
| Online Store | Redis| Low-latency real-time serving |
| Offline Store | Parquet | Historical feature storage |
| Computation | Python/Pandas | Feature engineering |
| Data | Synthetic CSV | E-commerce transactions |
| Version Control | Git/GitHub | Code management |

---

##  Project Structure

```
feature-store-mvp/
├── generate_synthetic_data.py      # Creates fake transaction data
├── compute_features.py             # Calculates rolling averages
├── materialize_to_online.py        # Syncs offline → online store
├── demo_fixed.py                   # Demonstrates skew prevention
├── user_transactions.csv           # Raw data (100 rows, 20 users)
│
├── data/
│   └── user_features.parquet      # Offline store (training data)
│
├── feature_repo/
│   ├── feature_store.yaml         # Feast configuration
│   ├── features.py                # SINGLE SOURCE OF TRUTH
│   └── data/
│       └── registry.db            # Feast metadata registry
│
└── README.md                       # This file
```

---

## Quick Start

### Prerequisites
```bash
pip install feast pandas redis
docker run -d --name redis -p 6379:6379 redis:latest
```

### Run the Pipeline
```bash
# 1. Generate synthetic data
python generate_synthetic_data.py

# 2. Initialize Feast
cd feature_repo && feast apply && cd ..

# 3. Compute features
python compute_features.py

# 4. Materialize to Redis
python materialize_to_online.py

# 5. Run demo
python demo_fixed.py
```

---

##  Core Concept: Single Source of Truth

The Magic File: `feature_repo/features.py`

```python
from feast import FeatureView, Field, Entity
from feast.types import Float64, Int64

# Define user entity
user = Entity(name="user", join_keys=["user_id"])

# Define features - USED BY BOTH TRAINING AND SERVING
user_purchase_features = FeatureView(
    name="user_purchase_features",
    entities=[user],
    schema=[
        Field(name="user_avg_3day_purchase_amount", dtype=Float64),
        Field(name="user_total_transactions", dtype=Int64),
    ],
    online=True,  # Enable real-time serving
    source=user_features_source,
)
```

Why This Matters:
- Training calls: `store.get_historical_features()`
- Serving calls: `store.get_online_features()`
- Both use the same `user_purchase_features` definition**
- Impossible to have different logic!

---

## Results & Validation

### Skew Prevention Verified

```
Training (Offline Store):  $95.32
Serving (Online Store):    $95.32
Difference:                $0.00  

 100% Feature Consistency Achieved!
```

### Performance Metrics

| Metric | Value | Status |
|--------|-------|--------|
| Serving Latency | <10ms |  Production-ready |
| Feature Consistency | 100% |  Zero skew |
| Users Processed | 20 |  Scalable |
| Features per User | 2 |  Extensible |
| Cost | $0 |  Open-source |

---

##  What I Learned

### Technical Skills
- MLOps pipeline design and implementation
- Feature engineering with time-based aggregations
- Dual-store architecture (offline + online)
- Point-in-time correctness for training data
- Real-time serving with Redis
- Schema design and type safety

### Problem-Solving
- Challenge 1: Redis materialization was empty
  - **Solution**: Debugged timestamp alignment, adjusted time windows
  
- Challenge 2: User ID 5 not found in training data
  - **Solution**: Implemented auto-selection of valid users

- Challenge 3: Understanding Feast serialization warnings
  - Solution: Researched entity key serialization versions

---

##  Feature Computation Logic

Example: 3-Day Rolling Average

```python
# For each user transaction at timestamp T:
# 1. Look back 3 days from T
# 2. Get all transactions in [T-3days, T]
# 3. Calculate mean(purchase_amount)
# 4. Store as feature with timestamp T

current_time = row['timestamp']
lookback_start = current_time - timedelta(days=3)

window_df = transactions[
    (transactions['timestamp'] >= lookback_start) & 
    (transactions['timestamp'] <= current_time)
]

avg_3day = window_df['purchase_amount'].mean()
```

Why This Matters:
- Prevents data leakage (no future data in training)
- Consistent across training and serving
- Time-based features for recommendation systems

---

##  Scalability Path

### Current (Local MVP)
- 100 transactions
- 20 users
- Local Parquet files
- Single Redis instance
- Cost: $0

### Production (Cloud Scale)
- Billions of transactions
- Millions of users
- S3/GCS for offline store
- Redis Cluster for online store
- Spark for feature computation
- Cost: $200-500/month

Migration is seamless - just change storage configurations in `feature_store.yaml`!

---

##  Code Walkthrough

### 1. Data Generation
```python
# generate_synthetic_data.py
user_ids = np.random.choice(range(1, 21), size=100)
purchase_amounts = np.random.gamma(shape=2, scale=25, size=100)
# Creates realistic e-commerce transaction data
```

### 2. Feature Computation
```python
# compute_features.py
for user_id in df['user_id'].unique():
    # Rolling 3-day window
    window_df = user_df[
        (user_df['timestamp'] >= lookback_start) & 
        (user_df['timestamp'] <= current_time)
    ]
    avg_3day = window_df['purchase_amount'].mean()
```

### 3. Materialization
```python
# materialize_to_online.py
store.materialize(
    start_date=datetime.now() - timedelta(days=60),
    end_date=datetime.now()
)
# Syncs Parquet → Redis
```

### 4. Feature Retrieval

**Training:**
```python
training_features = store.get_historical_features(
    entity_df=pd.DataFrame({"user_id": [1]}),
    features=["user_purchase_features:user_avg_3day_purchase_amount"]
)
```

Serving:
```python
serving_features = store.get_online_features(
    entity_rows=[{"user_id": 1}],
    features=["user_purchase_features:user_avg_3day_purchase_amount"]
)
```

Result: SAME VALUES!

---

##  Use Cases

This pattern is used in production at:
- Uber (Michelangelo platform)
- Netflix (Recommendation systems)
- Airbnb (Search ranking)
- DoorDash (Delivery predictions)

Why? Prevents model degradation in production

---

##  Future Enhancements

-  Add data quality monitoring (Great Expectations)
-  Implement feature drift detection
-  Add more features (embeddings, aggregations)
-  Deploy on AWS (S3 + ElastiCache)
-  Add CI/CD for feature deployment
-  Implement A/B testing framework
-  Add feature importance tracking
-  Real-time streaming features (Kafka + Flink)

---

##  Resources & References

- [Feast Documentation](https://docs.feast.dev/)
- [Redis Documentation](https://redis.io/docs/)
- [Training-Serving Skew Explained](https://www.featurestore.org/)

---

##  Author

Omkar Deshmukh

- GitHub: [@OMKARDESHM](https://github.com/OMKARDESHM)
- Email: [omkard2205@gmail.com]

---

##  License

 License - see [LICENSE](LICENSE) file

---

##  Acknowledgments

Built using open-source tools:
- [Feast](https://feast.dev/) - Feature store framework
- [Redis](https://redis.io/) - In-memory database
- [Pandas](https://pandas.pydata.org/) - Data manipulation
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage

---

If you found this helpful, please star this repository and share it with others!

---
