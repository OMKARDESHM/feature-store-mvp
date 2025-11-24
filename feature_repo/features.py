"""
Feast Feature Definitions for E-commerce Recommendation System
Defines entities, data sources, and feature views
"""
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, Int64

# Define the user entity
user = Entity(
    name="user",
    join_keys=["user_id"],
    description="User entity for e-commerce platform"
)

# Define the data source (offline store)
# NOTE: Path is relative to feature_repo directory, so use ../data/
user_features_source = FileSource(
    path="../data/user_features.parquet",
    timestamp_field="event_timestamp",
)

# Define feature view with 3-day rolling average purchase amount
user_purchase_features = FeatureView(
    name="user_purchase_features",
    entities=[user],
    ttl=timedelta(days=7),  # Features valid for 7 days
    schema=[
        Field(name="user_avg_3day_purchase_amount", dtype=Float64),
        Field(name="user_total_transactions", dtype=Int64),
    ],
    online=True,
    source=user_features_source,
    tags={"team": "ml_platform", "use_case": "recommendations"},
)
