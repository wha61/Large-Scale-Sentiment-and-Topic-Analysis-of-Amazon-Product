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

CMPT732_AUV/
│
├── data/
│   └── raw/                 # raw data files (not committed if too large)
│
├── file/
│   ├── CMPT732_Final_Project_Proposal.pdf
│   └── sflogo.png
│
├── notebooks/
│
├── src/
│
├── output/
│   └── results/             # saved analysis results
│
└── README.md


## Prerequisites