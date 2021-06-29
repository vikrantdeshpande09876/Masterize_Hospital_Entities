# Masterizing data based on text-similarity

## Preface

Generating and maintaining a Master set from assets reported by various third-party vendors, internal source systems, and systems integrated in-cases of 
acquisitions and mergers, at one central data-hub repository is challenging. 
A ‘single version of truth’ maintained for associated entities across the entire organization’s data sources, can help orchestrate collaboration between multiple cross-functional channels of the business.

## Motivation

This repo focuses on masterizing clinical data in terms of hospitals/sites, that the pharmaceutical client managed. The datasets used in this repository are open-sourced and available for free at [pbpython: Master Data](https://github.com/chris1610/pbpython/tree/master/data).
For example- the same site “Kadlec Regional Medical Center”, might be reported differently as “Kadlec Clinic Hematology and Oncology” but with the same address, across the client’s source systems. Our goal is to identify a golden entity (Master Record) to which other duplicate records can be matched, and maintain their source-to-master linkage (Cross-Reference). Although industry-standard tools are available (Informatica, Oracle, SAP, etc.) that can be used with third-party collaborators like Address-Doctor-Service, or Dun&Bradstreet to retrieve the standardized asset data, this repo was intended to prove that open-source code and libraries can produce near-standardized results.
![Basic_deduplication_example](/Documentation/Research_Paper_Work/Basic_deduplication_example.jpg?raw=True)


## Overview

For de-duplicating records and generating a master set, we compute the similarity between two textual strings and determine if they are a probabilistic data match.
If two or more records seem to belong to the same golden entity i.e. the Master record, they are ‘linked’ together. The intuition behind this procedure is as follows:
> Within a dataset of n records, we must compare the 1st record with the remaining (n - 1) records, the 2nd record with the remaining (n - 2) records, and so on. Thus, there would be nC2 = ( n\*(n-1)/2 ) unique combinations to be considered.

> Between 2 different datasets of m and n records each, there will be ( m\*n ) such unique combinations.

The RecordLinkage library in R provides two main functions to generate ( n\*(n-1)/2 ) candidates for deduplication within a single dataset (hereafter called the _dedup_ function), or ( m\*n ) candidates for identifying duplicates between two different datasets (hereafter called the _linkage_ function)

Most importantly, machine-learning techniques like clustering or classification algorithms, weren’t applicable since there wasn’t a target variable/list to train or test on. Hence, the goal was to leverage Levenshtein similarity scores to compare text holistically and link records together.


A recursive algorithmic approach will first pass minibatches of a fixed size into the _dedup_ R-function and generate deduplicated master-datasets.
These deduplicated master-datasets would be compared against each other using the _linkage_ R-function. This is similar to the conventional level-order traversal of a binary tree using a queue, but in reverse, until each record is compared against every other. The motivation here is to prevent overuse of RAM, due to in-memory candidate pair computations.
![RECURSIVE PROCESSING](/Documentation/Research_Paper_Work/Recursive_Approach_Formalized.jpg?raw=True)


## Step-by-step guide to setup this data-pipeline

1.	Install GIT (This repo used 2.25.1 on Windows-10): [Git](https://git-scm.com/downloads)

2.1.	Install Python (This repo was built on 3.8.1 but is compatible with 2.5x): [Python](https://www.python.org/downloads/)

2.2.	Set up Jupyter Notebook using [Anaconda](https://www.anaconda.com/products/individual) or [Visual Studio Code](https://code.visualstudio.com/download) (VS Code has a Jupyter Notebook extension now)

2.3.	Set up Spark and Pyspark: [Apache PySpark for Windows 10](https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1)

2.4.	Install R and R-studio (This repo was built on 4.0.4 but is compatible with 3.4x): [R](https://www.r-project.org/)

3.	Download this repository: [Masterize_Hospital_Entities](https://github.com/vikrantdeshpande09876/Masterize_Hospital_Entities)

4.	Create a virtual environment for making a copy of your system-wide Python interpreter, in the directory for this repo:
```
> python -m venv myvirtualenv
```

5.	Activate this virtual environment. You need not perform step#4 each time for execution:
```
> myvirtualenv\Scripts\activate
```

6.	Install the application-specific dependencies by executing:
```
> pip install -r requirements.txt
```

9.	Set up your data in **_hospital_account_info_raw.csv_** with the expected structure.

10.	Construct your thresholds for individual text-comparison within **_config.py_**

11. Execute the **_Recursive_Python_Site_Master.py_** script.