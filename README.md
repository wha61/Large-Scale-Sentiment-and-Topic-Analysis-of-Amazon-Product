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
│   ├── sentiment/              # Core AI inference engine
│   └── web_dashboard/          # Interactive web dashboard for visualization
├── requirements.txt            # Python3 dependencies
└── run_pipeline.sh             # Master script to execute the full workflow
```
### Detailed File Descriptions

#### 1. Data Acquisition (`src/data/`)
* **`download_category.py`**: Uses the Hugging Face `datasets` library to download the raw "All_Beauty" Amazon review dataset and saves it as a Parquet file.

    * **Default Usage:** Downloads the "All_Beauty" category by default.
        ```bash
        python3 src/data/download_category.py
        ```
    * **Custom Category:** You can specify any other category name as an argument (e.g., "Electronics", "Toys_and_Games").
        ```bash
        python3 src/data/download_category.py Electronics
        ```
Available Categories
Use any of the names below as an argument for the downloader script (e.g., `python3 src/data/download_category.py Electronics`).
| Category Name | Scale / Notes |
| :--- | :--- |
| **`All_Beauty`** | **Small** (Recommended for testing) |
| `Amazon_Fashion` | Medium |
| `Appliances` | Small |
| `Arts_Crafts_and_Sewing` | Medium |
| `Automotive` | Medium |
| `Baby_Products` | Medium |
| `Beauty_and_Personal_Care` | **Large** |
| `Books` | **Very Large** (Requires High RAM) |
| `CDs_and_Vinyl` | Medium |
| `Cell_Phones_and_Accessories` | **Large** |
| `Clothing_Shoes_and_Jewelry` | **Very Large** |
| `Digital_Music` | Small |
| `Electronics` | **Very Large** |
| `Gift_Cards` | **Small** |
| `Grocery_and_Gourmet_Food` | Medium |
| `Handmade_Products` | Small |
| `Health_and_Household` | **Large** |
| `Health_and_Personal_Care` | Small |
| `Home_and_Kitchen` | **Very Large** |
| `Industrial_and_Scientific` | Small |
| `Kindle_Store` | **Large** |
| `Magazine_Subscriptions` | **Small** |
| `Movies_and_TV` | **Large** |
| `Musical_Instruments` | Small |
| `Office_Products` | Medium |
| `Patio_Lawn_and_Garden` | Medium |
| `Pet_Supplies` | Medium |
| `Software` | Small |
| `Sports_and_Outdoors` | **Large** |
| `Subscription_Boxes` | **Very Small** |
| `Tools_and_Home_Improvement` | **Large** |
| `Toys_and_Games` | Medium |
| `Video_Games` | Small |

> **Warning:** Categories marked as **Large** or **Very Large** contain tens of millions of reviews. Processing them may take significantly longer and require more memory.

#### 2. ETL & Inspection (`src/etl/`)
* **`clean_data.py`**: The main ETL script. It cleans the raw data by filtering null texts, converting Unix timestamps to standard datetime objects, extracting `year`/`month` features, and calculating text length.
* **`raw_data_checker.py`**: Utility to inspect the schema and sample rows of the raw downloaded data.
* **`cleaned_allbeautyy_checker.py`**: Verifies the schema and data quality after the cleaning process.
* **`sentiment_allbeauty_checker.py`**: Checks the final dataset after sentiment inference, calculating global metrics like MAE (Mean Absolute Error).

#### 3. Sentiment Inference (`src/sentiment/`)
* **`sentiment.py`**: **The Core Engine**. This script performs scalable, distributed sentiment analysis using **PySpark Pandas UDFs**. It loads a pre-trained BERT model (`nlptown/bert-base-multilingual-uncased-sentiment`) on worker nodes to generate `sentiment_star` (1-5 scale) and `sentiment_conf` (confidence score) for the entire dataset.

#### 4. Analysis (`src/analysis/`)
**Micro-Level Analysis:**
* **`mismatched.py`**: Identifies "conflicted" reviews where the user's star rating differs significantly from the text sentiment (e.g., Rating 5 vs. Sentiment 1). It extracts these anomalies for further topic modeling.
* **`topic_modeling_mismatched.py`**: Applies **BERTopic** to the extracted mismatched reviews to uncover latent themes, explaining *why* users leave conflicting feedback.
* **`detect_anomalous_users.py`**: Detects potential spammers (high volume, zero rating variance) and conflicted users (high rating but low sentiment score) by aggregating user behavior metrics.
* **`rating_vs_sentiment.py`**: Generates statistical summaries comparing user ratings vs. AI sentiment scores across the entire dataset to validate model alignment.

**Macro-Level Analysis:**
* **`macro_correlation.py`**: Aggregates sentiment scores by year and correlates them with external economic indicators (Inflation Rate) to identify macro-economic trends in consumer sentiment.

#### 5. Web Dashboard (`src/web_dashboard/`)
* **`app.py`**: Flask web application that provides an interactive dashboard for visualizing all analysis results. Features include:
    * Statistics overview (total reviews, mismatched count, topics, MAE)
    * Rating vs Sentiment interactive charts
    * Topic modeling visualization with top words
    * Mismatched reviews explorer with search functionality
    * Macro-economic correlation plots
* **`run_dashboard.bat`**: Windows batch script for easy dashboard startup
* **`run_dashboard.sh`**: Linux/Mac shell script for dashboard startup
* **`README.md`**: Detailed dashboard documentation
* **`QUICKSTART.md`**: Quick start guide for dashboard usage


## Prerequisites
Before running the project, ensure you have the following software installed on your system:
* **Python 3.8+**: Required for all analysis scripts.
    * Verify installation: `python3 --version`
* **Apache Spark**: Required for distributed ETL and scalable sentiment analysis.
    * Ensure `spark-submit` is available in your system PATH.
    * Verify installation: `spark-submit --version`
* **Java (JDK 8 or 11)**: Required by Apache Spark runtime.
    * Verify installation: `java -version`

> **Note:** If you are running this on a cluster (e.g., AWS EMR, Databricks) or a university server, these tools are likely pre-installed.

All dependencies have been stored in the requirements/txt file.
Run command below to install all
```command
pip3 install -r requirements.txt
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
    A [Download Data] -->|src/data/download_category.py| B 
    (Raw Parquet `data/raw/{category}_reviews`)

    B -->|src/etl/clean_data.py| C 
    (Cleaned Data `data/processed/{category}_clean`) 

    C -->|--test (Fast)| D[Test Mode: 1000 rows]
    C -->|(Full Data)| E[Full Mode: All rows]

    E or D --> F 
    (Sentiment Data `data/processed/{category}_sentiment`)

    F -->|src/analysis/macro_correlation.py| I
    (Macro Correlation `output/{category}_macro_correlation_plot.png`)

    F -->|src/analysis/detect_anomalous_users.py| J
    (Anomalous Users `output/{category}_suspicious_users_analysis`)

    F -->|src/analysis/rating_vs_sentiment.py| K
    (Rating vs Sentiment `output/{category}_rating_vs_sentiment`)

    F -->|src/analysis/mismatched.py| L
    (Mismatched Reviews `output/{category}_mismatched_csv`  &  `data/processed/{category}_mismatched`)

    L -->|src/analysis/topic_modeling_mismatched.py| M
    (Topic Modeling `output/{category}_topic_mismatched_barchart.html`  &  `output/{category}_mismatched_topics.csv` )

```

#### Phase 1: Data Preparation
1. Download Raw Data Downloads the "#category" dataset from Hugging Face.
```
python3 src/data/download_category.py {category}
```
2. ETL & Cleaning Cleans the raw data, filters nulls, and formats timestamps.

* Input: ```data/raw/{category}_reviews```
* Output: ```data/processed/{category}_clean```

```
spark-submit src/etl/clean_data.py {category}
```
#### Phase 2: Sentiment Inference
This step uses PySpark and BERT to generate sentiment scores. Choose one mode:

```
# Test Mode (Recommended for Debugging) Runs only on 1000 rows to verify the pipeline works quickly (< 2 mins).

spark-submit src/sentiment/sentiment.py {category} --test
```

```
# Full Production Mode Runs on the entire dataset. This may take hours depending on your hardware.

spark-submit src/sentiment/sentiment.py {category}
```

all mode will output data at
```data/processed/{category}_sentiment```


#### Phase 3: Analytics & Visualization
Once `Phase 2` is complete, run these scripts in any order to generate insights.

* User Behavior Analysis Detects "Spammers" (high volume, zero variance) and "Conflicted Users" (high rating, low sentiment).
```
spark-submit src/analysis/detect_anomalous_users.py {category}
```

* Macro-Economic Analysis Correlates sentiment trends with US Inflation rates (generates plot).
```
python3 src/analysis/macro_correlation.py {category}
```

* Consistency Check Calculates MAE and consistency between User Ratings (1-5) and Sentiment (1-5).
```
spark-submit src/analysis/rating_vs_sentiment.py {category}
```

* Mismatch Extraction Identifies reviews where Rating and Sentiment differ significantly (e.g., 5-star rating but negative text).
```
spark-submit src/analysis/mismatched.py {category}
```

#### Phase 4: Advanced Topic Modeling
Once `Mismatch Extraction Identifies` is complete,
Run BERTopic Analyzes the "mismatched" reviews to understand why users gave conflicting feedback.

```
python3 src/analysis/topic_modeling_mismatched.py {category}
```

output: `output/{category}_topic_mismatched_barchart.html `


#### Phase 5: Web Dashboard Visualization
After completing the analysis pipeline, you can launch an interactive web dashboard to visualize all the results.

**Prerequisites:**
- Flask is already included in `requirements.txt`
- Ensure you have completed at least Phase 2 (Sentiment Inference) and Phase 3 (Analytics) to generate the required output files

**Windows Users:**
```bash
# Option 1: Double-click the batch file
src/web_dashboard/run_dashboard.bat

# Option 2: Run from command line
cd src/web_dashboard
python app.py
```

**Linux/Mac Users:**
```bash
cd src/web_dashboard
python app.py
```

**Access the Dashboard:**
Open your web browser and navigate to:
```
http://localhost:5000
```

**Dashboard Features:**
- 📊 **Statistics Overview**: Total reviews, mismatched reviews count, topics identified, average MAE
- ⭐ **Rating vs Sentiment Analysis**: Interactive charts comparing user ratings with AI sentiment scores
- 🔍 **Topic Modeling Results**: Visualize discovered topics from BERTopic with top words
- ⚠️ **Mismatched Reviews**: Explore reviews where rating and sentiment differ significantly
- 🌍 **Macro-Economic Correlation**: View correlation between sentiment trends and inflation rates

**Required Output Files:**
The dashboard expects the following files in the `output/` directory:
- `rating_vs_sentiment_{category}/part-00000-*.csv` - Rating vs sentiment comparison data
- `mismatched_{category}_csv/part-00000-*.csv` - Mismatched reviews data
- `mismatched_topics.csv` - Topic modeling results
- `macro_correlation_plot.png` - Macro-economic correlation plot

> **Note:** If you see "Error loading data", make sure you've run the analysis pipeline (Phases 2-4) first to generate the required output files.

**Stop the Dashboard:**
Press `Ctrl+C` in the terminal to stop the Flask server.


#### Helper Tools (Optional for Debug)

* `spark-submit src/etl/raw_checker.py {category}` (Check raw download)

* `spark-submit src/etl/cleaned_checker.py {category}` (Check ETL output)

* `spark-submit src/etl/sentiment_checker.py {category}` (Check final model output & MAE)