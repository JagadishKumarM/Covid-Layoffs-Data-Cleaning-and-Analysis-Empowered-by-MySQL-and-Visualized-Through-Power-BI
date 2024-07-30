-- Creating a new database named Dataingestion
CREATE DATABASE Dataingestion;

-- Switching to the newly created database
USE DATABASE Dataingestion;

-- Creating or replacing a storage integration for accessing S3
CREATE OR REPLACE STORAGE INTEGRATION S3_Snowpipe_integration
ENABLED = TRUE
TYPE = 'EXTERNAL_STAGE'
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::73033****:role/S3_Snowpipe'
STORAGE_ALLOWED_LOCATIONS = ('s3://covid-layoffs');

-- Describing the storage integration to verify its configuration
DESC INTEGRATION S3_Snowpipe_integration;

-- Creating or replacing an internal stage that references the S3 bucket
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://covid****'
STORAGE_INTEGRATION = S3_Snowpipe_integration;

-- Listing files in the internal stage to verify accessibility
list @my_s3_stage;

-- Creating or replacing a file format definition for CSV files
CREATE OR REPLACE FILE FORMAT CSV_FILE
PARSE_HEADER = TRUE
TYPE = 'CSV'
FIELD_DELIMITER=','
FIELD_OPTIONALLY_ENCLOSED_BY='"';

-- Creating a table using a template inferred from the CSV schema
CREATE OR REPLACE TABLE layoffs USING TEMPLATE (
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
FROM TABLE (INFER_SCHEMA(
LOCATION => '@my_s3_stage/Covid-layoffs-Transformed-data.csv',
FILE_FORMAT => 'CSV_FILE')));

-- Method 1: Creating a Snowpipe to automate data loading
CREATE OR REPLACE PIPE S3_pipe
AUTO_INGEST = TRUE
AS
COPY INTO layoffs
FROM @my_s3_stage/Covid-layoffs-Transformed-data.csv
FILE_FORMAT = CSV_FILE
MATCH_BY_COLUMN_NAME='CASE_SENSITIVE';

-- Query to check for non-null values in specific columns of the table
SELECT * FROM layoffs
WHERE "total_laid_off" IS NOT NULL AND "percentage_laid_off" IS NOT NULL;

-- Showing the list of pipes to verify Snowpipe creation
SHOW PIPES;

-- Checking the status of the Snowpipe
SELECT SYSTEM$PIPE_STATUS('S3_PIPE');

-- Refreshing the Snowpipe to process new files
ALTER PIPE S3_pipe REFRESH;

-- Method 2: Creating a table manually and loading data using COPY INTO
CREATE TABLE layoff (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    "date" TEXT,
    "stage" TEXT,
    country TEXT,
    funds_raised_millions INT);

-- Copying data from the S3 stage into the manually created table
COPY INTO layoff
FROM @my_s3_stage/Covid-layoffs-Transformed-data.csv
FILE_FORMAT = CSV_FILE
MATCH_BY_COLUMN_NAME='CASE_SENSITIVE';

-- Query to retrieve and sort data from the manually created table
SELECT * FROM layoff
ORDER BY company;
