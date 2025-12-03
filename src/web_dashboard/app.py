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
OUTPUT_DIR = BASE_DIR / "output-cz"  # Changed to output-cz directory

def load_rating_vs_sentiment():
    """Load rating vs sentiment comparison data"""
    import glob
    csv_pattern = str(OUTPUT_DIR / "All_Beauty_rating_vs_sentiment" / "part-*.csv")
    csv_files = glob.glob(csv_pattern)
    if csv_files:
        df = pd.read_csv(csv_files[0])
        return df.to_dict('records')
    return []

def load_mismatched_reviews():
    """Load mismatched reviews data"""
    import glob
    csv_pattern = str(OUTPUT_DIR / "All_Beauty_mismatched_csv" / "part-*.csv")
    csv_files = glob.glob(csv_pattern)
    if csv_files:
        df = pd.read_csv(csv_files[0])
        # Limit to first 100 for performance
        return df.head(100).to_dict('records')
    return []

def load_topic_info():
    """Load topic modeling results"""
    csv_path = OUTPUT_DIR / "All_Beauty_mismatched_topics.csv"
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
    csv_pattern = str(OUTPUT_DIR / "All_Beauty_suspicious_users" / "part-*.csv")
    csv_files = glob.glob(csv_pattern)
    if csv_files:
        try:
            df = pd.read_csv(csv_files[0])
            return df.to_dict('records')
        except Exception as e:
            print(f"Error reading anomalous users CSV: {e}")
            return []
    return []

def load_data_processing_stats():
    """Load data processing scale statistics using Spark"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        import os
        
        spark = SparkSession.builder.appName("DataProcessingStats").getOrCreate()
        
        # Paths to different data stages
        raw_path = BASE_DIR / "data" / "raw" / "All_Beauty_reviews.parquet"
        clean_path = BASE_DIR / "data" / "processed" / "all_beauty_clean"
        sentiment_path = BASE_DIR / "data" / "processed" / "all_beauty_sentiment"
        
        stats = {
            'raw': {'count': 0, 'size_mb': 0, 'partitions': 0},
            'cleaned': {'count': 0, 'size_mb': 0, 'partitions': 0},
            'sentiment': {'count': 0, 'size_mb': 0, 'partitions': 0}
        }
        
        # Get raw data stats
        if raw_path.exists():
            try:
                df_raw = spark.read.parquet(str(raw_path))
                stats['raw']['count'] = df_raw.count()
                stats['raw']['partitions'] = df_raw.rdd.getNumPartitions()
                raw_size = os.path.getsize(raw_path) / (1024 * 1024)  # MB
                stats['raw']['size_mb'] = round(raw_size, 2)
                print(f"Raw data: {stats['raw']['count']} records, {stats['raw']['size_mb']} MB")
            except Exception as e:
                print(f"Error reading raw data: {e}")
                import traceback
                traceback.print_exc()
        
        # Get cleaned data stats
        if clean_path.exists():
            has_parquet = any(clean_path.rglob('*.parquet'))
            if has_parquet or (clean_path / "_SUCCESS").exists():
                try:
                    print(f"Reading cleaned data from: {clean_path}")
                    df_clean = spark.read.parquet(str(clean_path))
                    stats['cleaned']['count'] = df_clean.count()
                    stats['cleaned']['partitions'] = df_clean.rdd.getNumPartitions()
                    clean_size = sum(f.stat().st_size for f in clean_path.rglob('*') if f.is_file()) / (1024 * 1024)
                    stats['cleaned']['size_mb'] = round(clean_size, 2)
                    print(f"Cleaned data: {stats['cleaned']['count']} records, {stats['cleaned']['size_mb']} MB")
                except Exception as e:
                    print(f"Error reading cleaned data: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"Cleaned path exists but no parquet files found: {clean_path}")
        
        # Get sentiment data stats
        if sentiment_path.exists():
            has_parquet = any(sentiment_path.rglob('*.parquet'))
            if has_parquet or (sentiment_path / "_SUCCESS").exists():
                try:
                    print(f"Reading sentiment data from: {sentiment_path}")
                    df_sentiment = spark.read.parquet(str(sentiment_path))
                    stats['sentiment']['count'] = df_sentiment.count()
                    stats['sentiment']['partitions'] = df_sentiment.rdd.getNumPartitions()
                    sentiment_size = sum(f.stat().st_size for f in sentiment_path.rglob('*') if f.is_file()) / (1024 * 1024)
                    stats['sentiment']['size_mb'] = round(sentiment_size, 2)
                    print(f"Sentiment data: {stats['sentiment']['count']} records, {stats['sentiment']['size_mb']} MB")
                    
                    # Get additional Spark metrics
                    spark_context = spark.sparkContext
                    stats['spark_info'] = {
                        'app_name': spark_context.appName,
                        'spark_version': spark_context.version,
                        'default_parallelism': spark_context.defaultParallelism
                    }
                    
                    # Get data quality metrics
                    total_rows = stats['sentiment']['count']
                    if total_rows > 0:
                        null_counts = df_sentiment.select([
                            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) 
                            for c in ['text', 'rating', 'sentiment_star']
                        ]).collect()[0]
                        
                        stats['data_quality'] = {
                            'null_text': null_counts['text'],
                            'null_rating': null_counts['rating'],
                            'null_sentiment': null_counts['sentiment_star'],
                            'completeness': round((1 - (null_counts['text'] + null_counts['rating'] + null_counts['sentiment_star']) / (total_rows * 3)) * 100, 2)
                        }
                except Exception as e:
                    print(f"Error reading sentiment data: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"Sentiment path exists but no parquet files found: {sentiment_path}")
        else:
            print(f"Sentiment path does not exist: {sentiment_path}")
        
        # Get Spark info if not already set
        if 'spark_info' not in stats or not stats['spark_info']:
            try:
                spark_context = spark.sparkContext
                stats['spark_info'] = {
                    'app_name': spark_context.appName,
                    'spark_version': spark_context.version,
                    'default_parallelism': spark_context.defaultParallelism
                }
            except:
                pass
        
        spark.stop()
        
        # Calculate processing metrics
        if stats['raw']['count'] > 0 and stats['cleaned']['count'] > 0:
            stats['processing_metrics'] = {
                'retention_rate': round((stats['cleaned']['count'] / stats['raw']['count']) * 100, 2),
                'data_reduction': round(((stats['raw']['count'] - stats['cleaned']['count']) / stats['raw']['count']) * 100, 2)
            }
        
        print(f"Final stats: {stats}")
        return stats
        
    except Exception as e:
        print(f"Error loading data processing stats: {e}")
        import traceback
        traceback.print_exc()
        return {
            'raw': {'count': 0, 'size_mb': 0, 'partitions': 0},
            'cleaned': {'count': 0, 'size_mb': 0, 'partitions': 0},
            'sentiment': {'count': 0, 'size_mb': 0, 'partitions': 0}
        }

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

@app.route('/api/data-processing-stats')
def api_data_processing_stats():
    """API endpoint for data processing scale statistics"""
    try:
        data = load_data_processing_stats()
        print(f"Data processing stats loaded: {data}")
        return jsonify(data)
    except Exception as e:
        print(f"Error in API endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'raw': {'count': 0, 'size_mb': 0, 'partitions': 0},
            'cleaned': {'count': 0, 'size_mb': 0, 'partitions': 0},
            'sentiment': {'count': 0, 'size_mb': 0, 'partitions': 0},
            'error': str(e)
        }), 500

@app.route('/static/macro_correlation_plot.png')
def serve_macro_plot():
    """Serve macro correlation plot image"""
    plot_path = OUTPUT_DIR / "All_Beauty_macro_correlation_plot.png"
    if plot_path.exists():
        return send_from_directory(str(OUTPUT_DIR), "All_Beauty_macro_correlation_plot.png")
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

