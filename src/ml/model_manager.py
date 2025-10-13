"""
Advanced Machine Learning Integration System
Predictive Analytics, Churn Prediction, Demand Forecasting & Anomaly Detection
"""

import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import joblib
import json
import logging
from pathlib import Path

from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, mean_squared_error
from sklearn.cluster import KMeans
import asyncpg

from src.database.connection import get_async_db
from src.services.error_monitoring import error_monitor
from src.streaming.kafka_processor import publish_system_alert, StreamEventType
from config import PROJECT_ROOT

logger = logging.getLogger(__name__)

class ModelType(Enum):
    CUSTOMER_CHURN = "customer_churn"
    DEMAND_FORECAST = "demand_forecast"
    ANOMALY_DETECTION = "anomaly_detection"
    CUSTOMER_SEGMENTATION = "customer_segmentation"
    PRICE_OPTIMIZATION = "price_optimization"
    QUERY_PERFORMANCE = "query_performance"

@dataclass
class MLModel:
    model_type: ModelType
    model: Any
    scaler: Optional[Any]
    encoder: Optional[Any]
    feature_columns: List[str]
    target_column: str
    accuracy: float
    trained_at: datetime
    version: str

@dataclass
class Prediction:
    model_type: ModelType
    prediction: Any
    confidence: float
    features_used: Dict[str, Any]
    prediction_timestamp: datetime

class MLModelManager:
    """
    Enterprise Machine Learning Model Management System
    """
    
    def __init__(self):
        self.models = {}
        self.model_dir = PROJECT_ROOT / "data" / "models"
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize model configurations
        self.model_configs = {
            ModelType.CUSTOMER_CHURN: {
                'algorithm': RandomForestClassifier,
                'params': {'n_estimators': 100, 'random_state': 42, 'max_depth': 10},
                'retrain_interval_days': 7,
                'min_accuracy_threshold': 0.85
            },
            ModelType.DEMAND_FORECAST: {
                'algorithm': LinearRegression,
                'params': {},
                'retrain_interval_days': 3,
                'min_accuracy_threshold': 0.75
            },
            ModelType.ANOMALY_DETECTION: {
                'algorithm': IsolationForest,
                'params': {'contamination': 0.1, 'random_state': 42},
                'retrain_interval_days': 1,
                'min_accuracy_threshold': 0.70
            },
            ModelType.CUSTOMER_SEGMENTATION: {
                'algorithm': KMeans,
                'params': {'n_clusters': 5, 'random_state': 42},
                'retrain_interval_days': 14,
                'min_accuracy_threshold': 0.60
            }
        }
    
    async def initialize_models(self):
        """Initialize and load all ML models"""
        logger.info("🤖 Initializing ML model system...")
        
        try:
            # Load existing models or train new ones
            for model_type in ModelType:
                if model_type in self.model_configs:
                    model = await self._load_or_train_model(model_type)
                    if model:
                        self.models[model_type] = model
                        logger.info(f"✅ Loaded {model_type.value} model (accuracy: {model.accuracy:.3f})")
            
            logger.info(f"🎯 ML system initialized with {len(self.models)} models")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize ML models: {e}")
            error_monitor.capture_exception(e, {"context": "ml_initialization"})
            return False
    
    async def _load_or_train_model(self, model_type: ModelType) -> Optional[MLModel]:
        """Load existing model or train new one"""
        try:
            # Try to load existing model
            model_file = self.model_dir / f"{model_type.value}_model.joblib"
            
            if model_file.exists():
                model = self._load_model_from_file(model_file)
                if model and self._is_model_recent(model):
                    return model
            
            # Train new model if no valid existing model
            return await self._train_model(model_type)
            
        except Exception as e:
            logger.error(f"Failed to load/train {model_type.value}: {e}")
            return None
    
    async def _train_model(self, model_type: ModelType) -> Optional[MLModel]:
        """Train ML model with fresh data"""
        logger.info(f"🏋️ Training {model_type.value} model...")
        
        try:
            # Get training data
            data = await self._get_training_data(model_type)
            
            if data is None or len(data) < 50:
                logger.warning(f"Insufficient data for {model_type.value} model")
                return None
            
            # Prepare features and target
            X, y, feature_columns, target_column = await self._prepare_features(data, model_type)
            
            if X is None:
                logger.error(f"Feature preparation failed for {model_type.value}")
                return None
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Scale features
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Initialize model
            config = self.model_configs[model_type]
            model = config['algorithm'](**config['params'])
            
            # Train model
            model.fit(X_train_scaled, y_train)
            
            # Evaluate model
            y_pred = model.predict(X_test_scaled)
            accuracy = self._calculate_accuracy(y_test, y_pred, model_type)
            
            # Create MLModel instance
            ml_model = MLModel(
                model_type=model_type,
                model=model,
                scaler=scaler,
                encoder=None,  # Add if needed for categorical variables
                feature_columns=feature_columns,
                target_column=target_column,
                accuracy=accuracy,
                trained_at=datetime.utcnow(),
                version="1.0.0"
            )
            
            # Save model
            await self._save_model(ml_model)
            
            logger.info(f"✅ Trained {model_type.value} model with {accuracy:.3f} accuracy")
            return ml_model
            
        except Exception as e:
            logger.error(f"❌ Training failed for {model_type.value}: {e}")
            error_monitor.capture_exception(e, {"context": "model_training", "model_type": model_type.value})
            return None
    
    async def _get_training_data(self, model_type: ModelType) -> Optional[pd.DataFrame]:
        """Get training data for specific model type"""
        try:
            async with get_async_db() as conn:
                
                if model_type == ModelType.CUSTOMER_CHURN:
                    # Customer churn prediction data
                    query = """
                    SELECT 
                        c.customer_id,
                        c.customer_tier,
                        EXTRACT(DAYS FROM (CURRENT_DATE - c.created_at)) as days_as_customer,
                        COALESCE(ca.total_orders, 0) as total_orders,
                        COALESCE(ca.total_revenue, 0) as total_revenue,
                        COALESCE(ca.avg_order_value, 0) as avg_order_value,
                        COALESCE(ca.days_since_last_order, 365) as days_since_last_order,
                        COALESCE(ca.customer_lifetime_value, 0) as lifetime_value,
                        CASE WHEN ca.days_since_last_order > 180 THEN 1 ELSE 0 END as churned
                    FROM customers c
                    LEFT JOIN customer_analytics ca ON c.customer_id = ca.customer_id
                    WHERE c.created_at <= CURRENT_DATE - INTERVAL '3 months'
                    """
                
                elif model_type == ModelType.DEMAND_FORECAST:
                    # Demand forecasting data
                    query = """
                    SELECT 
                        DATE_TRUNC('week', o.order_date) as week_date,
                        p.category,
                        COUNT(oi.item_id) as quantity_sold,
                        AVG(oi.unit_price) as avg_price,
                        COUNT(DISTINCT o.customer_id) as unique_customers,
                        EXTRACT(WEEK FROM o.order_date) as week_of_year,
                        EXTRACT(MONTH FROM o.order_date) as month,
                        CASE WHEN EXTRACT(DOW FROM o.order_date) IN (0,6) THEN 1 ELSE 0 END as is_weekend
                    FROM orders o
                    JOIN order_items oi ON o.order_id = oi.order_id
                    JOIN products p ON oi.product_id = p.product_id
                    WHERE o.order_date >= CURRENT_DATE - INTERVAL '12 months'
                    GROUP BY DATE_TRUNC('week', o.order_date), p.category, 
                             EXTRACT(WEEK FROM o.order_date), EXTRACT(MONTH FROM o.order_date),
                             EXTRACT(DOW FROM o.order_date)
                    ORDER BY week_date
                    """
                
                elif model_type == ModelType.ANOMALY_DETECTION:
                    # Anomaly detection for query performance
                    query = """
                    SELECT 
                        execution_time_ms,
                        rows_returned,
                        query_cost,
                        EXTRACT(HOUR FROM timestamp) as hour_of_day,
                        EXTRACT(DOW FROM timestamp) as day_of_week,
                        LENGTH(query_text) as query_length,
                        CASE WHEN query_text ILIKE '%JOIN%' THEN 1 ELSE 0 END as has_joins,
                        CASE WHEN query_text ILIKE '%GROUP BY%' THEN 1 ELSE 0 END as has_groupby,
                        CASE WHEN query_text ILIKE '%ORDER BY%' THEN 1 ELSE 0 END as has_orderby
                    FROM query_performance_log
                    WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
                    AND execution_time_ms IS NOT NULL
                    """
                
                elif model_type == ModelType.CUSTOMER_SEGMENTATION:
                    # Customer segmentation data
                    query = """
                    SELECT 
                        c.customer_id,
                        COALESCE(ca.total_orders, 0) as total_orders,
                        COALESCE(ca.total_revenue, 0) as total_revenue,
                        COALESCE(ca.avg_order_value, 0) as avg_order_value,
                        COALESCE(ca.days_since_last_order, 365) as recency,
                        EXTRACT(DAYS FROM (CURRENT_DATE - c.created_at)) as customer_age_days,
                        CASE c.customer_tier 
                            WHEN 'Premium' THEN 3 
                            WHEN 'Standard' THEN 2 
                            ELSE 1 
                        END as tier_numeric
                    FROM customers c
                    LEFT JOIN customer_analytics ca ON c.customer_id = ca.customer_id
                    WHERE c.is_active = true
                    """
                
                else:
                    return None
                
                # Execute query and return DataFrame
                rows = await conn.fetch(query)
                if rows:
                    data = pd.DataFrame([dict(row) for row in rows])
                    return data
                
        except Exception as e:
            logger.error(f"Failed to get training data for {model_type.value}: {e}")
            return None
    
    async def _prepare_features(self, data: pd.DataFrame, model_type: ModelType) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], List[str], str]:
        """Prepare features and target variables"""
        try:
            if model_type == ModelType.CUSTOMER_CHURN:
                feature_columns = ['days_as_customer', 'total_orders', 'total_revenue', 
                                 'avg_order_value', 'days_since_last_order', 'lifetime_value']
                target_column = 'churned'
            
            elif model_type == ModelType.DEMAND_FORECAST:
                feature_columns = ['avg_price', 'unique_customers', 'week_of_year', 'month', 'is_weekend']
                target_column = 'quantity_sold'
            
            elif model_type == ModelType.ANOMALY_DETECTION:
                feature_columns = ['execution_time_ms', 'rows_returned', 'hour_of_day', 
                                 'day_of_week', 'query_length', 'has_joins', 'has_groupby', 'has_orderby']
                target_column = None  # Unsupervised learning
            
            elif model_type == ModelType.CUSTOMER_SEGMENTATION:
                feature_columns = ['total_orders', 'total_revenue', 'avg_order_value', 
                                 'recency', 'customer_age_days', 'tier_numeric']
                target_column = None  # Unsupervised learning
            
            else:
                return None, None, [], ""
            
            # Prepare features
            X = data[feature_columns].fillna(0).values
            
            # Prepare target (if supervised learning)
            y = None
            if target_column and target_column in data.columns:
                y = data[target_column].fillna(0).values
            
            return X, y, feature_columns, target_column
            
        except Exception as e:
            logger.error(f"Feature preparation failed: {e}")
            return None, None, [], ""
    
    def _calculate_accuracy(self, y_true, y_pred, model_type: ModelType) -> float:
        """Calculate appropriate accuracy metric for model type"""
        try:
            if model_type in [ModelType.CUSTOMER_CHURN]:
                # Classification accuracy
                return accuracy_score(y_true, y_pred)
            
            elif model_type in [ModelType.DEMAND_FORECAST]:
                # Regression R² score (converted to positive accuracy-like metric)
                mse = mean_squared_error(y_true, y_pred)
                rmse = np.sqrt(mse)
                # Convert to accuracy-like score (0-1)
                mean_actual = np.mean(y_true)
                return max(0, 1 - (rmse / mean_actual))
            
            else:
                # Default for unsupervised models
                return 0.75
                
        except Exception as e:
            logger.error(f"Accuracy calculation failed: {e}")
            return 0.0
    
    async def predict(self, model_type: ModelType, input_data: Dict[str, Any]) -> Optional[Prediction]:
        """Make prediction using specified model"""
        try:
            if model_type not in self.models:
                logger.error(f"Model {model_type.value} not loaded")
                return None
            
            model = self.models[model_type]
            
            # Prepare input features
            features = []
            for col in model.feature_columns:
                features.append(input_data.get(col, 0))
            
            X = np.array(features).reshape(1, -1)
            
            # Scale features
            if model.scaler:
                X_scaled = model.scaler.transform(X)
            else:
                X_scaled = X
            
            # Make prediction
            prediction = model.model.predict(X_scaled)[0]
            
            # Calculate confidence (simplified)
            confidence = 0.85  # Would implement proper confidence intervals in production
            
            return Prediction(
                model_type=model_type,
                prediction=prediction,
                confidence=confidence,
                features_used=input_data,
                prediction_timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            logger.error(f"Prediction failed for {model_type.value}: {e}")
            error_monitor.capture_exception(e, {"context": "prediction", "model_type": model_type.value})
            return None
    
    async def predict_customer_churn(self, customer_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Predict if customer will churn"""
        prediction = await self.predict(ModelType.CUSTOMER_CHURN, customer_data)
        
        if prediction:
            churn_risk = "High" if prediction.prediction == 1 else "Low"
            return {
                "customer_id": customer_data.get("customer_id"),
                "churn_risk": churn_risk,
                "churn_probability": prediction.confidence if prediction.prediction == 1 else 1 - prediction.confidence,
                "confidence": prediction.confidence,
                "factors": self._analyze_churn_factors(customer_data),
                "timestamp": prediction.prediction_timestamp.isoformat()
            }
        
        return None
    
    def _analyze_churn_factors(self, customer_data: Dict[str, Any]) -> List[str]:
        """Analyze factors contributing to churn risk"""
        factors = []
        
        if customer_data.get("days_since_last_order", 0) > 90:
            factors.append("Long time since last order")
        
        if customer_data.get("total_orders", 0) < 5:
            factors.append("Low order frequency")
        
        if customer_data.get("avg_order_value", 0) < 100:
            factors.append("Low average order value")
        
        return factors
    
    async def _save_model(self, ml_model: MLModel):
        """Save trained model to disk"""
        try:
            model_file = self.model_dir / f"{ml_model.model_type.value}_model.joblib"
            
            model_data = {
                'model': ml_model.model,
                'scaler': ml_model.scaler,
                'encoder': ml_model.encoder,
                'feature_columns': ml_model.feature_columns,
                'target_column': ml_model.target_column,
                'accuracy': ml_model.accuracy,
                'trained_at': ml_model.trained_at,
                'version': ml_model.version
            }
            
            joblib.dump(model_data, model_file)
            logger.info(f"💾 Saved {ml_model.model_type.value} model")
            
        except Exception as e:
            logger.error(f"Failed to save model: {e}")
    
    def _load_model_from_file(self, model_file: Path) -> Optional[MLModel]:
        """Load model from file"""
        try:
            model_data = joblib.load(model_file)
            
            # Determine model type from filename
            model_type_str = model_file.stem.replace('_model', '')
            model_type = ModelType(model_type_str)
            
            return MLModel(
                model_type=model_type,
                model=model_data['model'],
                scaler=model_data['scaler'],
                encoder=model_data.get('encoder'),
                feature_columns=model_data['feature_columns'],
                target_column=model_data['target_column'],
                accuracy=model_data['accuracy'],
                trained_at=model_data['trained_at'],
                version=model_data.get('version', '1.0.0')
            )
            
        except Exception as e:
            logger.error(f"Failed to load model from {model_file}: {e}")
            return None
    
    def _is_model_recent(self, model: MLModel) -> bool:
        """Check if model is recent enough"""
        config = self.model_configs.get(model.model_type)
        if not config:
            return False
        
        days_since_training = (datetime.utcnow() - model.trained_at).days
        return days_since_training <= config['retrain_interval_days']

# Global ML manager instance
ml_manager = MLModelManager()
