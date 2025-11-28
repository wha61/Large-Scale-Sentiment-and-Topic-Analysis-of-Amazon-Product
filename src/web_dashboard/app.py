"""
Flask Web Dashboard for Amazon Reviews Analysis
Host locally to view analysis results with interactive visualizations
"""

from flask import Flask, render_template, jsonify, send_from_directory
import pandas as pd
import os
import json
from pathlib import Path
import shutil

app = Flask(__name__)

# Base paths
BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / "output"

def load_rating_vs_sentiment():
    """Load rating vs sentiment comparison data"""
    csv_path = OUTPUT_DIR / "rating_vs_sentiment_all_beauty" / "part-00000-1e698bb6-ad4a-4c31-9434-6abe433d857c-c000.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        return df.to_dict('records')
    return []

def load_mismatched_reviews():
    """Load mismatched reviews data"""
    csv_path = OUTPUT_DIR / "mismatched_all_beauty_csv" / "part-00000-2cc212be-577a-47bf-bc96-2d512d869407-c000.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        # Limit to first 100 for performance
        return df.head(100).to_dict('records')
    return []

def load_topic_info():
    """Load topic modeling results"""
    csv_path = OUTPUT_DIR / "mismatched_topics.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        # Filter out outlier topic (-1)
        df = df[df['Topic'] != -1]
        return df.to_dict('records')
    return []

def load_sentiment_data():
    """Load full sentiment data for temporal and distribution analysis"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.appName("LoadSentimentData").getOrCreate()
        df = spark.read.parquet(str(BASE_DIR / "data" / "processed" / "all_beauty_sentiment"))
        
        # Get yearly trends
        yearly = df.groupBy("year").agg(
            F.avg("sentiment_star").alias("avg_sentiment"),
            F.avg("rating").alias("avg_rating"),
            F.count("*").alias("count")
        ).orderBy("year").toPandas()
        
        # Get monthly trends (sample for performance)
        monthly = df.groupBy("year", "month").agg(
            F.avg("sentiment_star").alias("avg_sentiment"),
            F.avg("rating").alias("avg_rating"),
            F.count("*").alias("count")
        ).orderBy("year", "month").limit(1000).toPandas()
        
        # Get product-level sentiment (top 20 by review count)
        product_sentiment = df.groupBy("asin").agg(
            F.avg("sentiment_star").alias("avg_sentiment"),
            F.avg("rating").alias("avg_rating"),
            F.count("*").alias("count")
        ).orderBy(F.col("count").desc()).limit(20).toPandas()
        
        spark.stop()
        
        return {
            'yearly': yearly.to_dict('records'),
            'monthly': monthly.to_dict('records'),
            'product': product_sentiment.to_dict('records')
        }
    except Exception as e:
        print(f"Error loading sentiment data: {e}")
        import traceback
        traceback.print_exc()
        return {'yearly': [], 'monthly': [], 'product': []}

def load_anomalous_users():
    """Load anomalous users data"""
    import glob
    csv_pattern = str(OUTPUT_DIR / "suspicious_users_analysis" / "part-*.csv")
    csv_files = glob.glob(csv_pattern)
    if csv_files:
        try:
            df = pd.read_csv(csv_files[0])
            return df.to_dict('records')
        except Exception as e:
            print(f"Error reading anomalous users CSV: {e}")
            return []
    return []

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/rating-vs-sentiment')
def api_rating_vs_sentiment():
    """API endpoint for rating vs sentiment data"""
    data = load_rating_vs_sentiment()
    return jsonify(data)

@app.route('/api/mismatched-reviews')
def api_mismatched_reviews():
    """API endpoint for mismatched reviews"""
    data = load_mismatched_reviews()
    return jsonify(data)

@app.route('/api/topics')
def api_topics():
    """API endpoint for topic modeling results"""
    data = load_topic_info()
    return jsonify(data)

@app.route('/api/stats')
def api_stats():
    """API endpoint for overall statistics"""
    rating_data = load_rating_vs_sentiment()
    mismatched_data = load_mismatched_reviews()
    topic_data = load_topic_info()
    
    stats = {
        'total_ratings': sum(int(r.get('count', 0)) for r in rating_data),
        'mismatched_count': len(mismatched_data),
        'topic_count': len(topic_data),
        'avg_mae': sum(r.get('avg_abs_diff', 0) * r.get('count', 0) for r in rating_data) / 
                   sum(r.get('count', 0) for r in rating_data) if rating_data else 0
    }
    return jsonify(stats)

@app.route('/api/temporal-trends')
def api_temporal_trends():
    """API endpoint for yearly and monthly trends"""
    data = load_sentiment_data()
    return jsonify(data)

@app.route('/api/sentiment-distribution')
def api_sentiment_distribution():
    """API endpoint for sentiment distribution by product/category"""
    data = load_sentiment_data()
    return jsonify({
        'product': data.get('product', []),
        'category': [{'category': 'All Beauty', 'avg_sentiment': 3.5, 'count': 1000}]  # Placeholder
    })

@app.route('/api/anomalous-users')
def api_anomalous_users():
    """API endpoint for anomalous users"""
    data = load_anomalous_users()
    return jsonify(data)

@app.route('/static/macro_correlation_plot.png')
def serve_macro_plot():
    """Serve macro correlation plot image"""
    plot_path = OUTPUT_DIR / "macro_correlation_plot.png"
    if plot_path.exists():
        return send_from_directory(str(OUTPUT_DIR), "macro_correlation_plot.png")
    return "Image not found", 404

if __name__ == '__main__':
    # Ensure output directory exists
    if not OUTPUT_DIR.exists():
        print(f"Warning: Output directory not found: {OUTPUT_DIR}")
    
    print("=" * 60)
    print("Starting Amazon Reviews Analysis Dashboard")
    print("=" * 60)
    print(f"Dashboard will be available at: http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)

