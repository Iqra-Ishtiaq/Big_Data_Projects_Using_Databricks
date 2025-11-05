# Clinical Trials Data Analysis – Spark SQL

This project explores a large-scale **clinical trial dataset** using **Apache Spark SQL** on Databricks, demonstrating scalable data processing and analysis techniques for semi-structured data. The goal is to extract actionable insights on clinical trial trends, study durations, conditions, sponsors, collaborators, and diabetes-related research in the United States.  



## Dataset
The dataset (`Clinicaltrial_16012025.csv`) contains **over 500,000 records** of clinical trials. Key fields include:  
- Study Type & Status  
- Conditions & Interventions  
- Sponsors & Collaborators  
- Start & Completion Dates  
- Enrollment  



## Objectives
- Load, clean, and preprocess the clinical trial dataset on Databricks  
- Explore trial types, completion status, and study durations  
- Identify top sponsors and collaborators over time  
- Analyze single vs multi-condition trials and intervention types  
- Examine trends in diabetes-related research  
- Compute descriptive statistics such as enrollment distribution  
- Use Spark SQL for efficient distributed processing and aggregation  



## Technologies Used
- **Databricks Notebooks**  
- **Apache Spark SQL & PySpark**  
- **Databricks File System (DBFS)**  
- **DataFrames & SQL Queries**  
- Visualization tools within Databricks (bar charts, combo charts, line plots)  



## Analysis Tasks
The analysis was divided into sections based on complexity:  
- **Data Exploration & Preprocessing:** Handling missing values, trimming whitespaces, standardizing dates, and adding completion flags  
- **Exploratory Data Analysis (EDA):** Study type distribution, trial durations, top sponsors, trends over time, condition and intervention frequency  
- **Assessment Questions:** Top trial types, most frequent conditions, average trial length, and yearly trends for diabetes-related trials  
- **Descriptive Statistics:** Enrollment summary (min, max, median, quartiles, standard deviation)  



## Key Insights
- **Completion Status:** ~97% of trials completed; 3% ongoing  
- **Trial Duration:** Interventional trials are most common (~33 months average); Expanded Access trials are longest (~49 months)  
- **Top Sponsors:** Assiut University, GlaxoSmithKline, Mayo Clinic, and NCI lead in trial volume  
- **Conditions & Interventions:** About 65% of trials focus on a single condition; drugs dominate interventions, followed by devices and biologicals  
- **Top Collaborators:** NCI, NIMH, NIA, NIDA, and NIH are most active (2020–2025)  
- **Enrollment Stats:** Median enrollment of 72, with a mean of 5,613 and high variability, indicating some very large-scale trials  
- **Diabetes Research:** Steady rise in completed diabetes trials until 2018, with decline in recent years, possibly due to ongoing trials or delayed data reporting  



## Visualizations
Visualizations enhance interpretability, including:  
- Distribution of completed vs ongoing trials  
- Average and maximum study durations by study type  
- Top sponsors and collaborators over time  
- Single vs multi-condition trial distribution  
- Intervention type distribution  
- Yearly trend of diabetes-related trials  


## Conclusion
This project demonstrates the practical application of **Spark SQL for big data analysis**. By processing over **half a million clinical trial records**, it uncovers patterns in trial types, durations, conditions, sponsors, collaborators, and research trends.  

The methodology can be extended to other large-scale healthcare datasets, supporting strategic decision-making in the pharmaceutical sector.  

**Note:** The full notebook includes all SQL queries, data cleaning steps, and visualizations for reference.  

