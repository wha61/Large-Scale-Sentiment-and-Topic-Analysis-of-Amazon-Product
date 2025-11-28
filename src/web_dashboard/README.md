# Amazon Reviews Analysis Web Dashboard

A local web dashboard for visualizing Amazon Reviews analysis results.

## Features

- **Interactive Charts**: Multiple Chart.js visualizations
- **Statistics Overview**: Key metrics at a glance
- **Rating vs Sentiment Analysis**: Compare user ratings with AI sentiment scores
- **Topic Modeling Results**: View discovered topics from BERTopic
- **Mismatched Reviews**: Explore reviews where rating and sentiment differ
- **Macro-Economic Correlation**: View correlation with inflation rates

## Installation

1. Install Flask (if not already installed):
```bash
pip install flask
```

Or install all requirements:
```bash
pip install -r requirements.txt
```

## Usage

1. Navigate to the web_dashboard directory:
```bash
cd src/web_dashboard
```

2. Run the Flask application:
```bash
python app.py
```

Or from project root:
```bash
python src/web_dashboard/app.py
```

3. Open your browser and navigate to:
```
http://localhost:5000
```

## Data Requirements

The dashboard expects the following files in the `output/` directory:

- `rating_vs_sentiment_all_beauty/part-00000-*.csv` - Rating vs sentiment comparison data
- `mismatched_all_beauty_csv/part-00000-*.csv` - Mismatched reviews data
- `mismatched_topics.csv` - Topic modeling results
- `macro_correlation_plot.png` - Macro-economic correlation plot

## Features Overview

### Statistics Dashboard
- Total reviews count
- Mismatched reviews count
- Topics identified
- Average MAE (Mean Absolute Error)

### Rating vs Sentiment
- Bar chart: Average sentiment by rating
- Doughnut chart: Review distribution
- Line chart: MAE by rating

### Topic Modeling
- Bar chart: Topic distribution
- Table: Detailed topic information with top words

### Mismatched Reviews
- Bar chart: Mismatch distribution
- Pie chart: Sentiment confidence distribution
- Searchable table: Sample mismatched reviews

### Macro Correlation
- Image: Yearly sentiment vs inflation correlation plot

## Troubleshooting

If you see "Error loading data":
1. Make sure you've run the analysis pipeline first
2. Check that output files exist in the `output/` directory
3. Verify file paths are correct

If the dashboard doesn't load:
1. Check that Flask is installed: `pip install flask`
2. Check that port 5000 is not in use
3. Check the console for error messages

