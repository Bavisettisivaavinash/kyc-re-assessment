# kyc-re-assessment
# Data Analytics competetion by Barclays

Attribute Matching and Recommendation for Bank Data

This project aims to compare and evaluate the accuracy of third-party data sources against a bank's internal database. Using PySpark and SequenceMatcher, we calculate the match ratios for various attributes (e.g., Bank Name, Address, City) between the bank's data and two external sources. The project then recommends which third-party source the bank should use for future data requests based on the overall matching percentages.

The project's primary goals are to:
	Clean and Prepare Data: Load and preprocess data from the bank and two third-party sources.
	Calculate Match Ratios: Use SequenceMatcher to compute similarity scores between corresponding attributes in the datasets.
	Evaluate Data Sources: Calculate the overall matching percentage for each attribute from both sources.
	Recommendation: Determine the most reliable data source based on the calculated match ratios.

Key Components:
1. Data Loading and Cleaning
	Data Sources: Three CSV files - one for the bank's database and two for third-party data sources.
	Spark Session: Create a Spark session and load the data into DataFrames.
	Data Cleaning: Ensure all attributes are in string format for accurate comparison.
2. Match Ratio Calculation
	SequenceMatcher UDF: Define a User Defined Function (UDF) in PySpark using Python's difflib.SequenceMatcher to calculate the similarity ratio between strings.
	Join DataFrames: Join the bank's DataFrame with each third-party DataFrame on a common identifier (e.g., ID) to align corresponding records.
	Compute Ratios: Apply the UDF to compute match ratios for each attribute.
3. Overall Matching Percentage
	Average Ratios: Calculate the average match ratio for each attribute across all records for both third-party sources.
	Comparison: Compare the overall matching percentages to determine which source is more reliable.
4. Recommendation
	Decision Logic: Recommend the data source with the highest overall matching percentages for most attributes.

Prerequisites:
	Python 3.x
	PySpark
	Difflib
	Running the Project
	Setup Spark Session: Initialize a Spark session.
	Load Data: Load the CSV files into DataFrames.
	Data Cleaning: Ensure all attributes are in string format.
	Match Ratio Calculation: Use the UDF to compute match ratios.
	Calculate Averages: Compute average match ratios for each attribute.
	Compare and Recommend: Compare the results and print the recommendation.
