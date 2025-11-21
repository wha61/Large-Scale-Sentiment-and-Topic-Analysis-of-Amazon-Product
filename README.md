<img src="./file/sfulogo.png" alt="drawing" width="250"/>

# Large-Scale Sentiment and Topic Analysis of Amazon Product Reviews


# 2025 Fall CMPT732: Programming for Big Data [Team: AUV]

## Table of Contents
1. [Project Members](#project-members)
2. [Course](#course)
3. [Overview](#overview)
4. [Proposal](#Proposal)
5. [Report](#Report)
5. [Prerequisites](#Prerequisites)


## Project Members
- **Chenzheng Li** - cla429@sfu.ca
- **Wenxiang He** - wha61@sfu.ca

## Course
This project is the final project for the course - [2025 Fall CMPT 732: Programming for Big Data 1](https://coursys.sfu.ca/2025fa-cmpt-732-g1/pages/).

## Overview

This project performs **large-scale sentiment, topic, and behavioral analysis** on the *Amazon Product Reviews 2023* dataset, which contains more than **571 million reviews** spanning nearly three decades of global e-commerce activity. Our objective is to uncover both **micro-level user opinion patterns** and **macro-level economic trends** using a unified and scalable NLP pipeline built on top of **PySpark**.

At the **micro level**, we analyze how consumers express opinions through review text. We compute sentiment polarity scores using **transformer-based sentiment models**, and extract dominant themes using **BERTopic**, ensuring that all NLP components rely on a consistent embedding space powered by **SentenceTransformers**. This unified architecture allows us to efficiently process reviews at scale across Spark partitions. The analysis includes:

- Sentiment distributions across products, brands, and categories  
- Monthly and yearly sentiment trends  
- Topic clusters that reveal common concerns (e.g., quality, durability, fit, value)  
- Behavioral anomaly detection to identify **fake or suspicious reviews**, including high-frequency reviewers, extreme sentiment patterns, and repetitive text content  

At the **macro level**, we combine aggregated consumer sentiment with external economic indicators such as the **World Bank CPI (inflation)** dataset. By aligning yearly sentiment trends with inflation fluctuations, we explore whether consumer emotions expressed in product reviews show meaningful correlations or lagging responses to broader economic conditions.

The entire workflow is implemented as a **distributed PySpark pipeline**, supporting scalable ETL, NLP inference, topic modeling, anomaly detection, and correlation analysis. By adopting a consistent transformer-based NLP stack and introducing temporal and behavioral analysis, this project addresses the instructor's feedback and provides a distinct analytical framing beyond standard Amazon review sentiment studies.

Ultimately, this project demonstrates how large-scale NLP, user behavior modeling, and macroeconomic data integration can generate novel insights into both **online consumer behavior** and **global economic trends**.

## [Proposal](files/CMPT732_Final_Project_Proposal.pdf) 
## [Report(WIP)]() 

## Project Structure

The project is organized into a modular pipeline, separating data acquisition, ETL processing, core model inference, and downstream analysis.

```text
.
├── data/                       # Storage for raw and processed parquet files
│   ├── raw/                    # Raw Amazon datasets (downloaded)
│   └── processed/              # Cleaned data and sentiment inference results
├── output/                     # Analysis results (CSV, HTML plots, Parquet extracts)
├── src/                        # Source code
│   ├── analysis/               # Analytical scripts (Micro & Macro level)
│   ├── data/                   # Data ingestion scripts
│   ├── etl/                    # Data cleaning and schema inspection
│   └── sentiment/              # Core AI inference engine
├── requirements.txt            # Python dependencies
└── run_pipeline.sh             # Master script to execute the full workflow
```
### Detailed File Descriptions

#### 1. Data Acquisition (`src/data/`)
* **`download_category_all_beauty.py`**: Uses the Hugging Face `datasets` library to download the raw "All_Beauty" Amazon review dataset and saves it as a Parquet file.

#### 2. ETL & Inspection (`src/etl/`)
* **`clean_all_beauty.py`**: The main ETL script. It cleans the raw data by filtering null texts, converting Unix timestamps to standard datetime objects, extracting `year`/`month` features, and calculating text length.
* **`raw_data_checker.py`**: Utility to inspect the schema and sample rows of the raw downloaded data.
* **`cleaned_allbeautyy_checker.py`**: Verifies the schema and data quality after the cleaning process.
* **`sentiment_allbeauty_checker.py`**: Checks the final dataset after sentiment inference, calculating global metrics like MAE (Mean Absolute Error).

#### 3. Sentiment Inference (`src/sentiment/`)
* **`sentiment_all_beauty.py`**: **The Core Engine**. This script performs scalable, distributed sentiment analysis using **PySpark Pandas UDFs**. It loads a pre-trained BERT model (`nlptown/bert-base-multilingual-uncased-sentiment`) on worker nodes to generate `sentiment_star` (1-5 scale) and `sentiment_conf` (confidence score) for the entire dataset.

#### 4. Analysis (`src/analysis/`)
**Micro-Level Analysis:**
* **`mismatched.py`**: Identifies "conflicted" reviews where the user's star rating differs significantly from the text sentiment (e.g., Rating 5 vs. Sentiment 1). It extracts these anomalies for further topic modeling.
* **`topic_modeling_mismatched.py`**: Applies **BERTopic** to the extracted mismatched reviews to uncover latent themes, explaining *why* users leave conflicting feedback.
* **`detect_anomalous_users.py`**: Detects potential spammers (high volume, zero rating variance) and conflicted users (high rating but low sentiment score) by aggregating user behavior metrics.
* **`rating_vs_sentiment.py`**: Generates statistical summaries comparing user ratings vs. AI sentiment scores across the entire dataset to validate model alignment.

**Macro-Level Analysis:**
* **`macro_correlation.py`**: Aggregates sentiment scores by year and correlates them with external economic indicators (Inflation Rate) to identify macro-economic trends in consumer sentiment.


## Prerequisites
All dependencies have been stored in the requirements/txt file.
Run command below to install all
```command
pip install -r requirements.txt

```


Requirement detail:
```
pyspark
pandas
numpy<2.0
torch
transformers
umap-learn==0.5.6
bertopic
matplotlib
seaborn
datasets
pyarrow
scikit-learn
```

## Execution Pipeline
```text
graph TD
    A [Download Data] -->|src/data/download_category_all_beauty.py| B 
    (Raw Parquet `data/raw/all_beauty_sentiment`)

    B -->|src/etl/clean_all_beauty.py| C 
    (Cleaned Data `data/processed/all_beauty_clean`) 

    C -->|--test (Fast)| D[Test Mode: 1000 rows]
    C -->|(Full Data)| E[Full Mode: All rows]

    E or D --> F 
    (Sentiment Data `data/processed/all_beauty_sentiment`)

    F -->|src/analysis/macro_correlation.py| I
    (Macro Correlation `output/macro_correlation_plot.png`)

    F -->|src/analysis/detect_anomalous_users.py| J
    (Anomalous Users `output/suspicious_users_analysis`)

    F -->|src/analysis/rating_vs_sentiment.py| K
    (Rating vs Sentiment `output/rating_vs_sentiment_all_beauty`)

    F -->|src/analysis/mismatched.py| L
    (Mismatched Reviews `output/mismatched_all_beauty_csv`  &  `data/processed/all_beauty_mismatched`)

    L -->|src/analysis/topic_modeling_mismatched.py| M
    (Topic Modeling `output/topic_mismatched_barchart.html`  &  `output/mismatched_topics.csv` )

```

#### Phase 1: Data Preparation
1. Download Raw Data Downloads the "All_Beauty" dataset from Hugging Face.
```
python src/data/download_category_all_beauty.py
```
2. ETL & Cleaning Cleans the raw data, filters nulls, and formats timestamps.

* Input: ```data/raw/all_beauty_reviews```
* Output: ```data/processed/all_beauty_clean```

```
spark-submit src/etl/clean_all_beauty.py
```
#### Phase 2: Sentiment Inference
This step uses PySpark and BERT to generate sentiment scores. Choose one mode:

```
# Test Mode (Recommended for Debugging) Runs only on 1000 rows to verify the pipeline works quickly (< 2 mins).

spark-submit src/sentiment/sentiment_all_beauty.py --test
```

```
# Full Production Mode Runs on the entire dataset. This may take hours depending on your hardware.

spark-submit src/sentiment/sentiment_all_beauty.py
```

all mode will output data at
```data/processed/all_beauty_sentiment```


#### Phase 3: Analytics & Visualization
Once `Phase 2` is complete, run these scripts in any order to generate insights.

* User Behavior Analysis Detects "Spammers" (high volume, zero variance) and "Conflicted Users" (high rating, low sentiment).
```
spark-submit src/analysis/detect_anomalous_users.py
```

* Macro-Economic Analysis Correlates sentiment trends with US Inflation rates (generates plot).
```
python src/analysis/macro_correlation.py
```

* Consistency Check Calculates MAE and consistency between User Ratings (1-5) and Sentiment (1-5).
```
spark-submit src/analysis/rating_vs_sentiment.py
```

* Mismatch Extraction Identifies reviews where Rating and Sentiment differ significantly (e.g., 5-star rating but negative text).
```
spark-submit src/analysis/mismatched.py
```

#### Phase 4: Advanced Topic Modeling
Once `Mismatch Extraction Identifies` is complete,
Run BERTopic Analyzes the "mismatched" reviews to understand why users gave conflicting feedback.

```
python src/analysis/topic_modeling_mismatched.py
```

output: `output/topic_mismatched_barchart.html `


#### Helper Tools (Optional for Debug)

* `spark-submit src/etl/raw_data_checker.py` (Check raw download)

* `spark-submit src/etl/cleaned_allbeautyy_checker.py` (Check ETL output)

* `spark-submit src/etl/sentiment_allbeauty_checker.py` (Check final model output & MAE)