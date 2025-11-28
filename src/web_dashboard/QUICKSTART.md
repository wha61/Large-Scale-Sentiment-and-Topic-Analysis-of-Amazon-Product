# Quick Start Guide

## Starting the Dashboard

### Windows Users
Double-click to run:
```
src/web_dashboard/run_dashboard.bat
```

Or use command line:
```bash
cd src/web_dashboard
python app.py
```

### Linux/Mac Users
```bash
cd src/web_dashboard
python app.py
```

## Accessing the Dashboard

Open your web browser and navigate to:
```
http://localhost:5000
```

## Features Overview

### 1. Statistics Overview
- Total reviews count
- Mismatched reviews count
- Topics identified
- Average MAE

### 2. Rating vs Sentiment Analysis
- **Bar Chart**: Average sentiment score by rating
- **Doughnut Chart**: Rating distribution
- **Line Chart**: MAE by rating

### 3. Topic Modeling Results
- **Bar Chart**: Topic distribution
- **Table**: Detailed topic information with top words

### 4. Mismatched Reviews Analysis
- **Bar Chart**: Mismatch distribution
- **Pie Chart**: Sentiment confidence distribution
- **Searchable Table**: Sample mismatched reviews (with search functionality)

### 5. Macro-Economic Correlation
- Displays yearly sentiment trends vs inflation rate correlation chart

## Notes

1. Make sure you have run the analysis pipeline to generate output data
2. If you see "Error loading data", check if the corresponding CSV files exist in the output folder
3. Press Ctrl+C to stop the server

