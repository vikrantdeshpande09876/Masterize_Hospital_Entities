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



## Step-by-step guide to setup this data-pipeline

1.	Install GIT (This repo used 2.25.1 on Windows-10): [Git](https://git-scm.com/downloads)

2.	Install Python (This repo was built on 3.8.1 but is compatible with 2.5x): [Python](https://www.python.org/downloads/)

3.	Set up Jupyter Notebook using [Anaconda](https://www.anaconda.com/products/individual) or [Visual Studio Code](https://code.visualstudio.com/download) (VS Code has a Jupyter Notebook extension now)

4.	Set up Spark and Pyspark: [Apache PySpark for Windows 10](https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1)

5.	Install R and R-studio (This repo was built on 4.0.4 but is compatible with 3.4x): [R](https://www.r-project.org/)

6.	Download this repository: [Masterize_Hospital_Entities](https://github.com/vikrantdeshpande09876/Masterize_Hospital_Entities)

7.	Create a virtual environment for making a copy of your system-wide Python interpreter, in the directory for this repo:
```
> python -m venv myvirtualenv
```

8.	Activate this virtual environment. You need not perform step#4 each time for execution:
```
> myvirtualenv\Scripts\activate
```

9.	Install the application-specific dependencies by executing:
```
> pip install -r requirements.txt
```

10.	Set up your input data in **_hospital_account_info_raw.csv_** with the expected structure.

11.	Construct your thresholds for individual text-comparison within **_config.py_**

12. Execute the **_Recursive_Python_Site_Master.py_** script:
```
> python Recursive_Python_Site_Master.py
```



## Implementation notes:

i. The output of these R-functions can be interpreted as the raw universe of potential duplicates for that minibatch; a DataFrame containing the following columns:
		[ Source-Id, Master-Id, Site-Name-Comparison-Score, State-Comparison-Score, City-Comparison-Score, Address-Comparison-Score, Postal-Code-Comparison-Score ]

ii. The source-record can match against multiple master-records with a total match-score ≥ 4. We choose the best match for incoming source-records based on the highest total score for all its potential master-records (a Greedy Approach).

iii. There are cyclic cases in these score outputs like-

		> Record B matches against Record A
		
		> Record C matches against Record B
		
	Ideally, we should transitively maintain:
	
		> Record C matches against Record A
		
	These cyclic occurrences may extend to upwards of 10-15 such transitive linkages, so handling them efficiently is crucial.

iv. Finally, from this list of cleaned-normalized-score-features, using basic set-theory we find the unique list of masters. Consider ‘SR_NUM_1' as the list of incoming Source-Ids, and 'SR_NUM_2' as the Master-Ids to which ‘SR_NUM_1’ should be linked based on match-score.
‘SR_NUM’ of the entire minibatch, will be the universe of records.
Union of 'SR_NUM_1' & 'SR_NUM_2' will be the universe of potential duplicates (UPD).
Stand-alone records in the current minibatch, are those which do not fall in this universe of potential duplicates (Non-UPD).
The final Master-records will be the union of Master-Ids and the Stand-alone Ids identified above.
[Set_Theory_equations](/Documentation/Research_Paper_Work/Set_Theory_equations.jpg?raw=True)
[Set_Theory](/Documentation/Research_Paper_Work/Set_Theory.jpg?raw=True)


v. A recursive algorithmic approach will first pass minibatches of a fixed size into the _dedup_ R-function and generate deduplicated master-datasets.
These deduplicated master-datasets would be compared against each other using the _linkage_ R-function. This is similar to the conventional level-order traversal of a binary tree using a queue, but in reverse, until each record is compared against every other. The motivation here is to prevent overuse of RAM, due to in-memory candidate pair computations.
![RECURSIVE PROCESSING](/Documentation/Research_Paper_Work/Recursive_Approach_Formalized.jpg?raw=True)


vi. The time taken for recursively processing a large batch is significantly lower than the time that would’ve been theoretically required for one-shot processing.
The following observations were taken by considering minibatches of size 5,000, on an AWS EC2 instance m5.4xlarge (64 GB RAM, and 16 vCPUs- each a single thread on a 3.1 GHz Intel Xeon Platinum 8175M processor):
![Execution_stats_Tableau_output](/Documentation/Research_Paper_Work/Execution_stats_Tableau_output.jpg?raw=True)
