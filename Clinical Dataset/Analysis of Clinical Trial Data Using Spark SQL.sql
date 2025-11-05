-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Assignment 2 Task 1 - Analysis of Clinical Trial Data Using Spark SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1. Introduction
-- MAGIC > This notebook explores a large-scale clinical trial dataset (Clinicaltrial_16012025.csv) containing over **500,000 records** of studies conducted in the United States. It includes key features such as **study type**, **status**, **funding source**, and **conditions**. The main goal is to perform an analysis using Apache Spark SQL to support strategic decision-making in the pharmaceutical sector. 
-- MAGIC >
-- MAGIC >The analysis will address key questions regarding clinical trial types, common medical conditions, study durations, and trends in diabetes-related research. The notebook ensures reproducibility, validates assumptions, and demonstrates expertise in SQL and distributed computing within a big data environment.
-- MAGIC >
-- MAGIC >**`Key Aspects:`**
-- MAGIC >
-- MAGIC > - Use SQL queries for semi-structured data.
-- MAGIC > - Use of Spark functions: **explode**, **split**, **months_between**.
-- MAGIC > - Visualizations using Databricks native tools.
-- MAGIC > - Thorough data exploration and pre-processing, including null handling and parsing of multi-valued fields
-- MAGIC > - Verification of assumptions, such as handling of multi-condition rows or case sensitivity
-- MAGIC > - Clean, modular code with detailed comments for clarity.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Data Import

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > This section outlines the steps taken to read, inspect, and prepare the dataset for Spark SQL queries.
-- MAGIC >
-- MAGIC >**2.1: Previewing the Data**
-- MAGIC >
-- MAGIC > - Before loading the dataset, a preview using **`dbutils.fs.head()`** is performed to identify the proper delimiter usage (commas), escaped characters, multi-line entries and quoted values. This ensures compatibility with Spark’s CSV reader.
-- MAGIC > 
-- MAGIC >
-- MAGIC > **2.2: Loading the Data**
-- MAGIC >  
-- MAGIC > The dataset is loaded using `spark.read.csv()` with the following options to ensure correctness.
-- MAGIC >    - **`header=true`**: Ensures that the first row is interpreted as column headers, allowing for correct labeling of data fields.
-- MAGIC >    - **`inferSchema=true`**: Automatically detects data types (e.g., string, integer, date).
-- MAGIC >    - **`quote='"'` and `escape='"'`**: Ensures fields containing commas or special characters are correctly parsed by treating quoted text as a single value.
-- MAGIC >    - **`multiLine=true`**: Allows parsing of fields that span across multiple lines.
-- MAGIC > 
-- MAGIC > **2.3: Exploring the Data**
-- MAGIC >   
-- MAGIC > - Explore structure and sample records by running the SQL query `SELECT * FROM clinical_trials`.
-- MAGIC >
-- MAGIC >
-- MAGIC >
-- MAGIC >**`Note`:**  **Spark DataFrames** is registered as **temporary views** in order to run **SQL queries** without creating permanent tables.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,2.1: Raw Data Inspection
-- MAGIC %python
-- MAGIC #Preview the first few bytes of the Clinical Trials CSV file to inspect raw data format
-- MAGIC dbutils.fs.head('/FileStore/tables/Clinicaltrial_16012025.csv')

-- COMMAND ----------

-- DBTITLE 1,2.2: Load the dataset
-- MAGIC %python
-- MAGIC # Load CSV and register as SQL view
-- MAGIC df = spark.read \
-- MAGIC     .option("header", "true") \
-- MAGIC     .option("inferSchema", "true") \
-- MAGIC     .option("quote", '"') \
-- MAGIC     .option("escape", '"') \
-- MAGIC     .option("multiLine", "true") \
-- MAGIC     .csv("/FileStore/tables/Clinicaltrial_16012025.csv")
-- MAGIC
-- MAGIC # Register as a temp view for SQL querying
-- MAGIC df.createOrReplaceTempView("clinical_trials")
-- MAGIC
-- MAGIC df.display()
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,2.3: Preview Data
-- View sample data
SELECT * FROM clinical_trials;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Data Pre-Processing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC > Preprocessing ensures clean, consistent, and analysis-ready data. The following steps are applied to prepare the clinical trials dataset.
-- MAGIC > 
-- MAGIC > **3.1: Schema Exploration** 
-- MAGIC >
-- MAGIC >   - The structure and data types of the `clinical_trials` table were examined using `DESCRIBE` to validate column availability and types for SQL operations
-- MAGIC > 
-- MAGIC > **3.2: Missing Value Profiling**
-- MAGIC >
-- MAGIC >   - A null count was computed across all major columns using `SUM` and `CASE`. This step identified fields such as `Acronym`, `Collaborators`, and `Interventions` with significant missing values
-- MAGIC > 
-- MAGIC >  **3.3: Null Handling with Placeholder Values**
-- MAGIC > 
-- MAGIC >   - Created a temporary view, `clinical_trials_no_nulls`, where nulls were replaced using `COALESCE()` with `'N/A'`. This retained all records while clearly marking incomplete fields, ensuring no loss of valuable data due to row deletion
-- MAGIC > 
-- MAGIC > **3.4: Verification of Data Cleaning**:  
-- MAGIC >   - Counted 'N/A' values in each column to verify missing data after cleanup in `clinical_trials_no_nulls` view
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,3.1: Schema Exploration
-- Display the schema of the clinical_trials table: column names, data types
DESCRIBE clinical_trials;

-- COMMAND ----------

-- DBTITLE 1,3.2: Missing Value Profiling
-- Count the number of NULL (missing) values in each column of the clinical_trials table
SELECT 
  SUM(CASE WHEN `NCT Number` IS NULL THEN 1 ELSE 0 END) AS `NCT Number`,
  SUM(CASE WHEN `Study Title` IS NULL THEN 1 ELSE 0 END) AS `Study Title`,
  SUM(CASE WHEN Acronym IS NULL THEN 1 ELSE 0 END) AS Acronym,
  SUM(CASE WHEN `Study Status` IS NULL THEN 1 ELSE 0 END) AS `Study Status`,
  SUM(CASE WHEN Conditions IS NULL THEN 1 ELSE 0 END) AS Conditions,
  SUM(CASE WHEN Interventions IS NULL THEN 1 ELSE 0 END) AS Interventions,
  SUM(CASE WHEN Sponsor IS NULL THEN 1 ELSE 0 END) AS Sponsor,
  SUM(CASE WHEN Collaborators IS NULL THEN 1 ELSE 0 END) AS Collaborators,
  SUM(CASE WHEN `Funder Type` IS NULL THEN 1 ELSE 0 END) AS `Funder Type`,
  SUM(CASE WHEN `Study Type` IS NULL THEN 1 ELSE 0 END) AS `Study Type`,
  SUM(CASE WHEN `Study Design` IS NULL THEN 1 ELSE 0 END) AS `Study Design`,
  SUM(CASE WHEN `Start Date` IS NULL THEN 1 ELSE 0 END) AS `Start Date`,
  SUM(CASE WHEN `Completion Date` IS NULL THEN 1 ELSE 0 END) AS `Completion Date`
FROM clinical_trials;

-- COMMAND ----------

-- DBTITLE 1,3.3: Null Handling with Placeholder Values
-- Create a temporary view where all NULL values in selected columns are replaced with 'N/A'
CREATE OR REPLACE TEMP VIEW clinical_trials_no_nulls AS
SELECT
  COALESCE(`NCT Number`, 'N/A') AS `NCT Number`,
  COALESCE(`Study Title`, 'N/A') AS `Study Title`,
  COALESCE(Acronym, 'N/A') AS Acronym,
  COALESCE(`Study Status`, 'N/A') AS `Study Status`,
  COALESCE(Conditions, 'N/A') AS Conditions,
  COALESCE(Interventions, 'N/A') AS Interventions,
  COALESCE(Sponsor, 'N/A') AS Sponsor,
  COALESCE(Collaborators, 'N/A') AS Collaborators,
  COALESCE(Enrollment, 'N/A') AS Enrollment,
  COALESCE(`Funder Type`, 'N/A') AS `Funder Type`,
  COALESCE(`Study Type`, 'N/A') AS `Study Type`,
  COALESCE(`Study Design`, 'N/A') AS `Study Design`,
  COALESCE(`Start Date`, 'N/A') AS `Start Date`,
  COALESCE(`Completion Date`, 'N/A') AS `Completion Date`
FROM clinical_trials;

-- COMMAND ----------

-- DBTITLE 1,3.4: Verification of Null Handling in Cleaned Data
-- Count the number of 'N/A' values (originally NULLs replaced) in each column to verify the extent of missing data after cleanup in the clinical_trials_no_nulls view
SELECT 
  SUM(CASE WHEN Acronym IS NULL THEN 1 ELSE 0 END) AS Acronym,
  SUM(CASE WHEN `Study Status` IS NULL THEN 1 ELSE 0 END) AS `Study Status`,
  SUM(CASE WHEN Conditions IS NULL THEN 1 ELSE 0 END) AS Conditions,
  SUM(CASE WHEN Interventions IS NULL THEN 1 ELSE 0 END) AS Interventions,
  SUM(CASE WHEN Sponsor IS NULL THEN 1 ELSE 0 END) AS Sponsor,
  SUM(CASE WHEN Collaborators IS NULL THEN 1 ELSE 0 END) AS Collaborators,
  SUM(CASE WHEN `Funder Type` IS NULL THEN 1 ELSE 0 END) AS `Funder Type`,
  SUM(CASE WHEN `Study Type` IS NULL THEN 1 ELSE 0 END) AS `Study Type`,
  SUM(CASE WHEN `Study Design` IS NULL THEN 1 ELSE 0 END) AS `Study Design`,
  SUM(CASE WHEN `Start Date` IS NULL THEN 1 ELSE 0 END) AS `Start Date`,
  SUM(CASE WHEN `Completion Date` IS NULL THEN 1 ELSE 0 END) AS `Completion Date`
FROM clinical_trials_no_nulls;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.5: Trim WhiteSpaces 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC > 
-- MAGIC > **3.5.1: Trim Whitespace**
-- MAGIC > - Extra spaces can lead to inconsistent text entries, which may cause problems during analysis. Therfore, apply the `TRIM()` function to remove any leading or trailing spaces from key columns such as **Study Type**, **Study Status**, and **Funder Type**.This ensures consistent string matching, making it easier to compare values and perform accurate analysis
-- MAGIC >
-- MAGIC > **3.5.2: Creating a Cleaned View**
-- MAGIC >
-- MAGIC > - Creating a temporary view called `clinical_trials_trimmed`, which stores the cleaned version of the data. The **CREATE** OR **REPLACE** ensures that if the view already exists, it will be replaced with the new version
-- MAGIC >
-- MAGIC > **3.5.3: Verifying the Cleaned Data**
-- MAGIC >
-- MAGIC >  - Displays the first 10 records to confirm that the whitespace removal is successful

-- COMMAND ----------

select * from clinical_trials_no_nulls

-- COMMAND ----------

-- DBTITLE 1,3.5.1: Trim WhiteSpace
-- Compare original vs trimmed values to fix inconsistent text entries
SELECT DISTINCT
  `Study Type` AS `Study Type (Before)`,
  TRIM(`Study Type`) AS `Study Type (After)`,
  `Study Status` AS `Study Status (Before)`,
  TRIM(`Study Status`) AS `Study Status (After)`,
  `Funder Type` AS `Funder Type (Before)`,
  TRIM(`Funder Type`) AS `Funder Type (After)`
from clinical_trials_no_nulls

-- COMMAND ----------

-- DBTITLE 1,3.5.2: Creating a Cleaned Trim View
-- Trim spaces from key columns to ensure clean and consistent values
CREATE OR REPLACE TEMP VIEW clinical_trials_trimmed AS
SELECT
 `NCT Number`,
 `Study Title`,
  Acronym,
  TRIM(`Study Status`) AS `Study Status`,
  Conditions,
  Interventions,
  Sponsor,
  Collaborators,
  Enrollment,
  TRIM(`Funder Type`) AS `Funder Type`,
  TRIM(`Study Type`) AS `Study Type`,
  `Study Design`,
  `Start Date`,
  `Completion Date`
FROM clinical_trials_no_nulls;

-- COMMAND ----------

-- DBTITLE 1,3.5.3: Verifying the cleaned data
-- Verifying the cleaned data
SELECT * FROM clinical_trials_trimmed LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.6. Clean Date Formats and Add Completion Indicator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC >**3.6.1: Standardize Dates and Add Has_Completed Flag**
-- MAGIC >
-- MAGIC > It converts timestamp columns (i.e: **Start Date**, **Completion Date**) into a simpler date format and to introduce a flag that indicates whether the trial has a valid **completion date**. It's a boolean value.
-- MAGIC >
-- MAGIC > -  If the **Completion Dat**e is not NULL, the flag is set to **TRUE**
-- MAGIC > - If the **Completion Date** is NULL, the flag is set to **FALSE**
-- MAGIC >
-- MAGIC > This ensures that the dataset is cleaner and more manageable for analysis, especially when working with dates and filtering based on completion status.
-- MAGIC >
-- MAGIC > **3.6.2: Preview Data**
-- MAGIC >
-- MAGIC > - Displaying the record for top 1000 entries in the dataset
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,3.6.1: Standardize Dates and Add Has_Completed Flag
CREATE OR REPLACE TEMP VIEW ClinicalTrials_ParsedDates AS
SELECT *,
  
  -- Try parsing to date
  TRY_CAST(`Start Date` AS DATE) AS StartDateFormatted,
  TRY_CAST(`Completion Date` AS DATE) AS CompletionDateFormatted,

  -- Completion flag
  CASE 
    WHEN `Completion Date` IS NOT NULL 
         AND TRIM(`Completion Date`) != '' 
         AND LOWER(`Completion Date`) NOT IN ('n/a', 'na', 'unknown', 'null') 
    THEN TRUE 
    ELSE FALSE 
  END AS HasValidCompletionDate

FROM clinical_trials_trimmed;


-- COMMAND ----------

-- DBTITLE 1,3.6.2: Preview Data
-- Retrieve the first 1000 rows from the ClinicalTrials_ParsedDates view 
SELECT * FROM ClinicalTrials_ParsedDates LIMIT 1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Exploratory Data Analysis (EDA) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > **Exploratory Data Analysis (EDA)** is used to explore and understand clinical trial data, helping to uncover meaningful insights and patterns for further analysis.

-- COMMAND ----------

-- Describing the cleaned data
DESCRIBE clinical_trials

-- COMMAND ----------

select * from clinical_trials

-- COMMAND ----------

-- Total records in the dataset
SELECT COUNT(*) AS Total_Trials FROM ClinicalTrials_ParsedDates;

-- COMMAND ----------

-- Count of NULLs in key columns from original data
SELECT 
  COUNT(*) AS total_rows,
  SUM(CASE WHEN `Study Type` IS NULL THEN 1 ELSE 0 END) AS null_study_type,
  SUM(CASE WHEN `Study Status` IS NULL THEN 1 ELSE 0 END) AS null_status,
  SUM(CASE WHEN `Conditions` IS NULL THEN 1 ELSE 0 END) AS null_conditions,
  SUM(CASE WHEN `Start Date` IS NULL THEN 1 ELSE 0 END) AS null_start_date,
  SUM(CASE WHEN `Completion Date` IS NULL THEN 1 ELSE 0 END) AS null_completion_date
FROM clinical_trials;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.1: Completed vs Ongoing Clinical Trials
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > It displays the distribution of clinical trials by their completion status:
-- MAGIC > - **Completed trials:** `505,989`, making up `96.8%` of the total
-- MAGIC > - **Ongoing trials:** `16,671` accounting for `3.2%`
-- MAGIC > 
-- MAGIC > **Result**:
-- MAGIC This shows that nearly all clinical trials in the dataset have been completed, with only a small fraction still 0n-going.

-- COMMAND ----------

-- Selects the trial status (Completed or Ongoing) and the count of trials in each category
SELECT
  CASE 
    -- Checks if the CompletionDateFormatted is not null, meaning the trial is completed
    WHEN CompletionDateFormatted IS NOT NULL THEN 'Completed'
    -- If CompletionDateFormatted is null, it indicates an ongoing trial
    ELSE 'Ongoing'
  END AS Trial_Status,  -- Assigns the result of the case condition to Trial_Status
  COUNT(*) AS Trial_Count  -- Counts the number of trials in each status category
FROM ClinicalTrials_ParsedDates  -- From the ClinicalTrials_ParsedDates table
GROUP BY 
  CASE 
    -- Repeats the case condition for grouping trials by status
    WHEN CompletionDateFormatted IS NOT NULL THEN 'Completed'
    ELSE 'Ongoing'
  END;  -- Groups by the result of the case condition

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.2: Study Duration by Study Type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC >
-- MAGIC > The query examines clinical trial data by **_Study type_**, calculating the average and maximum durations, along with the total number of studies in each category.
-- MAGIC >
-- MAGIC > **Result**
-- MAGIC > - **_Expanded Access_** trials have the longest average duration `(48.6 months)`. Despite their longer duration, Expanded Access trials have the lowest count.
-- MAGIC >- **_Observational trials_** show extreme maximum durations. 
-- MAGIC >- **_Interventional trials_** have the shortest average duration of `33.4 months`,  and the highest number of studies `(399,888)`
-- MAGIC > - A combo chart highlights the contrast between trial length and study volume. Bars represent **avg_duration_months(duration)**. Line overlays **study_count(volume)**.

-- COMMAND ----------

-- Select relevant fields: study type, average & max duration, and count of studies
SELECT 
  `Study Type`,
  -- Calculate average duration in months, rounded to 1 decimal
  ROUND(AVG(MONTHS_BETWEEN(`Completion Date`, `Start Date`)), 1) AS avg_duration_months,
  -- Calculate maximum duration in months, rounded to 1 decimal
  ROUND(MAX(MONTHS_BETWEEN(`Completion Date`, `Start Date`)), 1) AS max_duration_months,
  -- Count total studies for each study type
  COUNT(*) AS study_count
FROM ClinicalTrials_ParsedDates
-- Filter out records with missing dates
WHERE `Start Date` IS NOT NULL 
  AND `Completion Date` IS NOT NULL
-- Group results by type of study
GROUP BY `Study Type`
-- Sort the results by average duration, descending
ORDER BY avg_duration_months DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.3: Top 10 Sponsors by Number of Trials

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > The query retrieves the top 10 sponsors with the highest number of clinical trial
-- MAGIC >
-- MAGIC > **Result**
-- MAGIC > - **_Assiut University_** leads with `3,927` trials, followed closely by **_Cairo University_** with `3,608` trials and **_GlaxoSmithKline_**  with `3,527` trials. 
-- MAGIC > - **PFizer** (`3,133` trials), as well as key research institutions like **_Mayo Clinic_** (`2,947` trials) and the **_National Cancer Institute (NCI)_**(`3,477` trials).
-- MAGIC > - The sponsor with the lowest count is **`Novartis Pharmaceuticals`** with `2,466` trials.

-- COMMAND ----------

SELECT 
  Sponsor,
  COUNT(*) AS num_trials
FROM ClinicalTrials_ParsedDates
GROUP BY Sponsor
ORDER BY num_trials DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.4: Trials Over Time (Start Year Trend)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > The query counts how many clinical trials started each year based on the **_StartDateFormatted_** field.
-- MAGIC >
-- MAGIC > **Result**
-- MAGIC >
-- MAGIC > - Sharp increase in trial activity since **1990**, especially post-2000.
-- MAGIC > - Peak in **2021** with `37,540` trials.
-- MAGIC >
-- MAGIC > - Recent years **_(2020–2024)_** show consistently high counts `(~33k–37k)`.
-- MAGIC >- Some future years **_(2025–2030, 2040, etc.)_** appear, possibly due to: Upcoming/planned trials
-- MAGIC >-  Early years **_(1900s–1970s)_** show very few trials — likely historical entries or anomalies

-- COMMAND ----------

-- Count the number of clinical trials started each year
SELECT 
  YEAR(StartDateFormatted) AS Start_Year,   -- Extract the year from the trial's start date
  COUNT(*) AS Trial_Count                   -- Count the number of trials for each year
FROM ClinicalTrials_ParsedDates
WHERE StartDateFormatted IS NOT NULL       -- Ensure the start date is available
GROUP BY YEAR(StartDateFormatted)          -- Group the data by year
ORDER BY Start_Year;                       -- Sort the results chronologically


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.5: Sponsor Contribution over Time (Top 5 Sponsors Only)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Selects the annual clinical trial counts for the top 5 sponsors (by total trial volume), grouped by year.
-- MAGIC > 
-- MAGIC > **Result**
-- MAGIC >
-- MAGIC > - Shows trends over time for these top sponsors.
-- MAGIC > 
-- MAGIC > - **_National Cancer Institute (NCI)_** leads consistently in recent years.
-- MAGIC > 
-- MAGIC > - **_GlaxoSmithKline_** and **_Assiut University_** peaked around `2020`.
-- MAGIC > 
-- MAGIC > - Early years had low activity; trial counts rose sharply after `2000`.
-- MAGIC > 
-- MAGIC > - Recent dip **_(2024–2025)_** likely due to partial/incomplete data collection

-- COMMAND ----------

-- Identify the top 5 sponsors based on the number of clinical trials
WITH TopSponsors AS (
  SELECT Sponsor
  FROM ClinicalTrials_ParsedDates
  GROUP BY Sponsor
  ORDER BY COUNT(*) DESC  -- Sort sponsors by the number of trials in descending order
  LIMIT 5                 -- Select only the top 5 sponsors
)

-- Count the number of clinical trials per year for each of the top 5 sponsors
SELECT 
  YEAR(StartDateFormatted) AS Year,   -- Extract the year from the start date
  Sponsor,
  COUNT(*) AS TrialCount              -- Count how many trials each sponsor had in that year
FROM ClinicalTrials_ParsedDates
WHERE StartDateFormatted IS NOT NULL  -- Exclude records with missing start dates
  AND Sponsor IN (SELECT Sponsor FROM TopSponsors) -- Keep only top 5 sponsors
GROUP BY Year, Sponsor
ORDER BY Year;                         -- Sort results chronologically by year


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.6: Distribution of Single vs Multi-Condition Clinical Trials

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Classifies clinical trials as either Single Condition or Multi-Condition based on the number of conditions listed per trial.
-- MAGIC >
-- MAGIC > **Result**
-- MAGIC > 
-- MAGIC > - **_Single Condition trials:_** `341,234`
-- MAGIC > 
-- MAGIC > - **_Multi-Condition trials:_** `181,426`
-- MAGIC > - About `65%` of trials focus on a single condition, while `35%` involve multiple conditions, indicating a significant share of trials deal with more complex scenarios

-- COMMAND ----------

-- Classify clinical trials based on whether they involve a single condition or multiple conditions
SELECT 
  CASE 
    WHEN SIZE(split(Conditions, '\\|')) > 1 THEN 'Multi-Condition'  -- If there is more than one condition, label as 'Multi-Condition'
    ELSE 'Single Condition'                                          -- Otherwise, label as 'Single Condition'
  END AS ConditionComplexity,
  COUNT(*) AS Count                                                  -- Count how many trials fall into each category
FROM ClinicalTrials_ParsedDates
GROUP BY ConditionComplexity                                         -- Group results by the condition complexity classification


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.7: Intervention Type Distribution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > This query categorizes each intervention in clinical trials into types such as **_Drug_**, **_Device_**, **_Procedure_**, **_Biological_**, and _**Behavioral**_ by analyzing keywords within the Interventions field.
-- MAGIC >
-- MAGIC > Result
-- MAGIC >
-- MAGIC > - **_Drug interventions_** dominates with (`374,826 trials`) , but a large number of interventions fall under "Other", suggesting room for better standardization or more detailed classification.
-- MAGIC > -  **_Device_** `(89,261)` and **_Biological_** `(43,411)` interventions are also prominent, indicating significant research in medical technology and biologically based treatments
-- MAGIC >
-- MAGIC > - **_Procedures_** `(5,871)` and **_Behavioral_** `(632)` interventions are relatively rare, possibly due to narrower application areas or less structured reporting

-- COMMAND ----------

-- Categorize interventions in clinical trials by type based on keywords
SELECT 
  CASE 
    WHEN LOWER(Intervention) LIKE '%drug%' THEN 'Drug'              -- If intervention contains 'drug', classify as Drug
    WHEN LOWER(Intervention) LIKE '%device%' THEN 'Device'          -- If it contains 'device', classify as Device
    WHEN LOWER(Intervention) LIKE '%surgery%' THEN 'Procedure'      -- If it contains 'surgery', classify as Procedure
    WHEN LOWER(Intervention) LIKE '%behaviour%' THEN 'BEHAVIORAL'   -- If it contains 'behaviour', classify as Behavioral
    WHEN LOWER(Intervention) LIKE '%biological%' THEN 'BIOLOGICAL'  -- If it contains 'biological', classify as Biological
    ELSE 'Other'                                                    -- Otherwise, classify as Other
  END AS Intervention_Type,
  COUNT(*) AS Count                                                 -- Count number of interventions per type
FROM (
  SELECT explode(split(Interventions, '\\|')) AS Intervention       -- Split multiple interventions using pipe (|) and flatten them
  FROM ClinicalTrials_ParsedDates
)
GROUP BY Intervention_Type                                          -- Group results by intervention type
ORDER BY Count DESC;                                                -- Sort by descending count


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.8: Top 5 Collaborators in Clinical Trials (2020–2025)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > The query identifies the top 5 collaborators in clinical trials from 2020 to 2025 by parsing the **Collaborators** field, and counting trial participation by year.
-- MAGIC > 
-- MAGIC > **Results**
-- MAGIC > 
-- MAGIC > - **_National Cancer Institute (NCI)_** consistently leads in collaborations, though its involvement has declined steadily from `2020` (over 400 trials) to `2025` (just above 100).
-- MAGIC > 
-- MAGIC > - Other top collaborators like **_NIMH_**, _**NIA**_, _**NIDA**_, and **_NIH_** also saw moderate fluctuations with a noticeable drop in `2025`, possibly due to reduced trial activity.
-- MAGIC > 

-- COMMAND ----------

-- Explode collaborators and extract year
WITH exploded_raw AS (
  SELECT 
    year(to_date(`StartDateFormatted`)) AS Year,  -- Extract year from StartDateFormatted
    explode(split(Collaborators, '\\|')) AS raw_collaborator  -- Split multiple collaborators into individual rows
  FROM ClinicalTrials_ParsedDates
  WHERE Collaborators IS NOT NULL                                      -- Filter out null collaborators
    AND StartDateFormatted IS NOT NULL                                 -- Filter out null start dates
),

-- Clean up collaborator names and filter by recent years
exploded AS (
  SELECT 
    Year,
    trim(raw_collaborator) AS Collaborator                             -- Remove leading/trailing whitespace from collaborator names
  FROM exploded_raw
  WHERE trim(raw_collaborator) IS NOT NULL                             -- Remove null or empty strings
    AND trim(raw_collaborator) != 'N/A'                                -- Remove placeholder values
    AND Year BETWEEN year(current_date()) - 5 AND year(current_date()) -- Keep only records from the last 5 years
),

-- Identify the top 5 most frequent collaborators
top_5_collaborators AS (
  SELECT Collaborator
  FROM exploded
  GROUP BY Collaborator
  ORDER BY COUNT(*) DESC                                               -- Sort by number of collaborations
  LIMIT 5                                                              -- Take the top 5 collaborators
)

-- Count the number of collaborations per year for top 5 collaborators
SELECT 
  e.Year,
  e.Collaborator,
  COUNT(*) AS Count                                -- Count of trials per yearper collaborator
FROM exploded e
JOIN top_5_collaborators t ON e.Collaborator = t.Collaborator --Join to keep only top 5collaborators
GROUP BY e.Year, e.Collaborator
ORDER BY e.Collaborator, e.Year;             -- Order results for readability


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.9: Descriptive statistics for Enrollments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > This query provides summary statistics about trial enrollment.
-- MAGIC > 
-- MAGIC > - Gives  the distribution **(min**, **max**, **median**, **quartiles**, and **standard deviation)**.
-- MAGIC > 
-- MAGIC > - It analyzed **500,942 trials**, revealing a **median** enrollment of `72`, but a much higher **average** of `5,613`. 
-- MAGIC > 

-- COMMAND ----------

-- Descriptive statistics for clinical trial enrollments
SELECT 
  COUNT(*) AS total_trials,                          -- Total number of trials with valid enrollment data
  MIN(Enrollment) AS min_enrollment,                 -- Minimum enrollment size
  MAX(Enrollment) AS max_enrollment,                 -- Maximum enrollment size
  ROUND(AVG(Enrollment), 2) AS avg_enrollment,       -- Average (mean) enrollment size, rounded to 2 decimals
  PERCENTILE(Enrollment, 0.5) AS median_enrollment,  -- Median (50th percentile) enrollment
  PERCENTILE(Enrollment, 0.25) AS q1,                -- First quartile (25th percentile)
  PERCENTILE(Enrollment, 0.75) AS q3,                -- Third quartile (75th percentile)
  STDDEV(Enrollment) AS std_dev                      -- Standard deviation to measure variability
FROM ClinicalTrials_ParsedDates
WHERE Enrollment IS NOT NULL AND Enrollment > 0;     -- Only include trials with valid, positive enrollment numbers


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5. Assessment Solution 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part 1
-- MAGIC
-- MAGIC > `Question`
-- MAGIC >
-- MAGIC > **List all the clinical trial types (as contained in the Type column of the data) along with their frequency, sorting the results from most to least frequent**
-- MAGIC >
-- MAGIC >`Explanation`
-- MAGIC > 
-- MAGIC > The query is designed to count the frequency of each clinical trial type in the dataset, sorting them from most to least frequent. The result shows that:
-- MAGIC >
-- MAGIC > - **_Interventional Trials_** dominate with **_399,888_** occurrences, reflecting their widespread use in clinical research
-- MAGIC >
-- MAGIC > - **_Observational Trials_** also represent a significant portion, with **_120,906_** records, indicating frequent studies that track conditions without interventions
-- MAGIC >
-- MAGIC > - **_Expanded Access_** and **_N/A_** values represent a small subset of trials, with **_966_** and _**900**_ occurrences, possibly related to regulatory exceptions or incomplete data
-- MAGIC >
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,5.1: Clinical Trial Type Frequency
-- List all clinical trial types along with their frequency, sorted from most to least frequent

SELECT 
  `Study Type`, -- Selects the 'Study Type' column to identify each trial type
  COUNT(*) AS Frequency -- Counts the number of occurrences for each trial type
FROM ClinicalTrials_ParsedDates
GROUP BY `Study Type` -- Groups the results by 'Study Type' to calculate the frequency of each type
ORDER BY Frequency DESC -- Orders the results by frequency in descending order, showing the most common types first


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part 2 
-- MAGIC > `Question:`
-- MAGIC >
-- MAGIC >**The top 10 conditions along with their frequency (note, that the Condition column can contain multiple conditions in each row, so you will need to separate these out and count each occurrence separately)**
-- MAGIC >
-- MAGIC > `Explaination`
-- MAGIC >
-- MAGIC > The query identifies the top 10 most frequent conditions in clinical trials by splitting the Conditions column; which contains multiple conditions per row, allowing each condition to be counted separately, and sorting them in descending order. The results show that: 
-- MAGIC > - **_Healthy_**: The most frequently mentioned condition, with a significant count of **_10,309_** instances, possibly reflecting studies focused on healthy volunteers or control groups
-- MAGIC >
-- MAGIC > - Cancer-related conditions, such as **_Breast Cancer (7,941 instances)_**, **_Prostate Cancer (4,071 instances)_**, and general Cancer, are prominently featured, suggesting that cancer-related trials are highly prevalent in the dataset
-- MAGIC >
-- MAGIC > - Chronic health conditions such as **_Obesity_ (6,954)**, **_Stroke_(4484)**, **_Hypertension_(4,256)**, and **_Depression_(4,196)** are also frequently studied, indicating that a substantial number of clinical trials focus on these common health problems
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,5.2: Top 10 Most Frequent Conditions in Clinical Trials
-- Explode and count top 10 conditions
SELECT
  TRIM(condition) AS Condition, -- Removes leading and trailing spaces from conditions
  COUNT(*) AS Frequency         -- Counts the occurrences of each condition
FROM (
   -- Inner query: Split the 'Conditions' column into individual conditions and explode them into separate rows
  SELECT
    EXPLODE(SPLIT(`Conditions`, '\\|')) AS condition -- Splits conditions and creates separate rows for each condition
  FROM ClinicalTrials_ParsedDates
  WHERE `Conditions` IS NOT NULL -- Ensures that only rows with conditions are considered
)
GROUP BY TRIM(condition) -- Groups the results by the trimmed condition values
ORDER BY Frequency DESC -- Orders the results by frequency, from most to least frequent
LIMIT 10; -- Limits the results to the top 10 conditions


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part 3
-- MAGIC > `Question` 
-- MAGIC >
-- MAGIC >  **For studies with an end date, calculate the mean clinical trial length in months**
-- MAGIC >
-- MAGIC >`Explanation`
-- MAGIC >
-- MAGIC > The query calculates:
-- MAGIC >
-- MAGIC > - The average duration of clinical trials in months by measuring the time difference between the **start** and **completion** dates.
-- MAGIC > - The average trial months is **_35.57_**, offers insight into the typical length of time needed for trials to reach completion

-- COMMAND ----------

-- DBTITLE 1,5.3: Clinical trial length
-- Select the average trial length in months
SELECT 
--Calculate the average of the differences in months between completion and start dates
  ROUND(AVG(MONTHS_BETWEEN(CompletionDateFormatted, StartDateFormatted)), 2) AS Average_Trial_Length_Months
FROM ClinicalTrials_ParsedDates
-- Filter out rows where either the Completion Date or Start Date is NULL
WHERE CompletionDateFormatted IS NOT NULL 
-- This ensures that only trials with both valid dates are included in the calculation
  AND StartDateFormatted IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part 4
-- MAGIC > **`Question`**
-- MAGIC >
-- MAGIC > **From the studies with a non-null completion date and a status of ‘Completed’ in the Study Status, calculate how many of these related to Diabetes each year. Display the trend over time in an appropriate visualisation. (For this you can assume all relevant studies will contain an exact match for ‘Diabetes’ or ‘diabetes’ 
-- MAGIC > in the Conditions column.)**
-- MAGIC >
-- MAGIC >**`Explanation`**
-- MAGIC >
-- MAGIC > The query calculates the number of diabetes-related studies per year from completed clinical trials with non-null completion dates. It filters studies containing "diabetes" and excludes "non-diabetic" conditions using `REGEXP` operator. 
-- MAGIC >
-- MAGIC > **`Key Findings`**
-- MAGIC > - The line graph shows a clear timeline of diabetes-related research activity, highlighting a slow start, a significant rise, peak productivity years, and a sharp recent decline that may be due to data lag or incomplete future entries.
-- MAGIC > - **1988–2000**: Minimal research activity, with very few completed diabetes trials recorded.
-- MAGIC >
-- MAGIC > - **2001–2004**: A gradual rise begins, indicating growing attention toward diabetes research.
-- MAGIC >
-- MAGIC > - **2005–2010**: Sharp increase in completed trials, showing a boom in clinical research efforts.
-- MAGIC >
-- MAGIC > - **2011–2018**: Peak period for diabetes studies; steady and high volume of completed trials.
-- MAGIC > 
-- MAGIC > - **2019–2021**: Slight decline, likely influenced by the COVID-19 pandemic's impact on research.
-- MAGIC >
-- MAGIC > - **2022–2023**: Further drop in numbers, possibly due to delayed completions or data updates.
-- MAGIC >
-- MAGIC > - **2024–2025**: Sharp decline to zero, likely because trials are ongoing or data is not yet available

-- COMMAND ----------

-- DBTITLE 1,5.4: Diabetes Trials: Yearly Trend
SELECT 
  YEAR(CompletionDateFormatted) AS year,                 -- Extracts the year from the completion date
  COUNT(*) AS diabetes_studies_count                     -- Counts the number of studies for each year
FROM 
  ClinicalTrials_ParsedDates AS ec
WHERE 
  ec.CompletionDateFormatted IS NOT NULL                 -- Ensures the completion date is present
  AND UPPER(ec.`Study Status`) = 'COMPLETED'             -- Filters only studies that are marked as COMPLETED
  AND LOWER(ec.Conditions) LIKE '%diab%'                 -- Includes studies related to diabetes (case-insensitive)
  AND LOWER(ec.Conditions) NOT REGEXP '\\bnon[\\s-]*diab[a-z]*\\b'  -- Excludes studies mentioning 'non-diabetic' or similar terms
GROUP BY 
  YEAR(ec.CompletionDateFormatted)                       -- Groups results by year
ORDER BY 
  year;                                                  -- Sorts results in ascending order by year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6. Conclusion
-- MAGIC >
-- MAGIC >
-- MAGIC > This analysis demonstrates the effectiveness of using **Apache Spark SQL** for large-scale clinical trial data analysis in the United States. By processing over half a million records, we were able to uncover meaningful patterns and trends within the healthcare research landscape.
-- MAGIC >
-- MAGIC > - **Data exploration and preprocessing**: The analysis inlcudes inspecting of the dataset structure, handling missing values, and parsing multi-valued fields such as `Conditions`. Proper **date formatting** and data cleaning were essential to ensure accuracy and consistency throughout the analysis.
-- MAGIC > - **Filtering and grouping techniques**: Applied targeted filters ncluding the use of  **regular expressions** to exclude irrelevant data and organized trials by completion year to reveal trends in research activity over time
-- MAGIC >
-- MAGIC > - **Trend analysis**: The findings revealed a steady rise in the number of clinical trials from the early 2000s, peaking between **2011 and 2018**, with a decline in more recent years likely due to **COVID-19 disruptions** and delays in reporting.
-- MAGIC >
-- MAGIC > - **Data visualization**: Visual representations is used to enhance the understanding and effectively highlight key trends observed in the analysi.
-- MAGIC >
-- MAGIC > - **Trial type analysis**: Interventional studies emerged as the most prevalent, highlighting their central role in clinical research.
-- MAGIC >
-- MAGIC > - **Condition frequency analysis**: The top 10 most commonly studied conditions were extracted from complex entries, revealing areas of concentrated medical interest.
-- MAGIC >
-- MAGIC > - **Trial duration calculation**: We determined the average duration of trials (in months) for studies with complete timelines, offering a practical reference point for future planning.
-- MAGIC >
-- MAGIC > - **Focus on diabetes research**: By isolating completed trials related to diabetes, we observed a clear upward trend in research focus until 2018, followed by a slight drop, potentially linked to global events.
-- MAGIC >
-- MAGIC > Overall, this study showcases the value of big data tools like **Spark SQL** in generating actionable insights for public health. The methods applied here lay a strong foundation for further exploration into other medical conditions, geographic trends, or trial phases, aiding strategic decision-making in the pharmaceutical sector.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC