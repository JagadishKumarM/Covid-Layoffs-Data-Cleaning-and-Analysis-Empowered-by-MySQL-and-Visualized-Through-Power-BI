/* -----------------------------------------------------STEP 1--> CREATE AND ORGANIZE DATABASE ------------------------------------------------------ */

CREATE DATABASE layoffs;
USE layoffs;


ALTER TABLE layoffs RENAME layoff;
DESC layoff;

SELECT * FROM layoff;

/*Creating and intserting a copy of the orignaly data/Table as a good practice method*/

-- Creating a new table 'layoff_copy' with the same structure as 'layoff'
CREATE TABLE layoff_copy 
LIKE layoff;

-- Displaying the contents of 'layoff_copy' to verify its structure
SELECT * 
FROM layoff_copy;

-- Inserting data into 'layoff_copy' from 'layoff'
INSERT INTO layoff_copy
SELECT * FROM layoff;

-- Describing the structure of 'layoff_copy' to confirm its attributes
DESCRIBE layoff_copy;


/*  ------------------------------------------------------STEP 2---> REMOVING THE DUPLICATES ------------------------------------------------------ */

/* Figure out the DUPLICATES  */
/* Note: We have quoted the date column as `date` as date is a keyword in MySql */

SELECT *,count(company) AS Duplicates
FROM layoff_copy 
GROUP BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
HAVING count(company)>1;

SELECT *
FROM layoff_copy 
WHERE company="Yahoo";

USE layoffs;

/* Using ROW_NUMBER() with CTE to Identify and Remove Duplicates */

SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_copy;

WITH CTE
AS (SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS row_num
FROM layoff_copy)
SELECT * FROM cte WHERE row_num>1;


/* The above queries help identify duplicates in the table. However, to remove the duplicates, 
we need to create another copy of the table ("layoff_copy2") and add a column named "row_num". 
This is necessary because the temporary "row_num" generated by the CTE or count function cannot be used for deletion. 
Adding a permanent "row_num" column will allow us to effectively identify and remove duplicate records. */

CREATE TABLE `layoff_copy2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE layoff_copy2;
/* These queries inserts data from layoff_copy into layoff_copy2, along with an additional row_num column that assigns a unique number to each row */

CREATE TABLE layoff_copy2
LIKE layoff_copy;

ALTER TABLE layoff_copy2
ADD COLUMN row_num int;

INSERT INTO layoff_copy2
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS row_num
FROM layoff_copy;

DELETE 
FROM layoff_copy2
WHERE row_num>1;

/*  The above code auto assigns ROW_NUMBER() to the column row_num implicitly as it's auto understood by the complier, we can do the same explicitly--

INSERT INTO layoff_copy2 (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num)
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
FROM layoff_copy; 
*/

SELECT * 
FROM layoff_copy2;


/*  ------------------------------------------------------ STEP3----> DATA ENRICHMENT VIA STANDARDIZING DATA ------------------------------------------------------ */

/* REMOVING any Leading or Trailing spaces for the columns required */

UPDATE layoff_copy2 
SET company=TRIM(company);

UPDATE layoff_copy2 
SET location=TRIM(location);

UPDATE layoff_copy2 
SET industry=TRIM(industry);

UPDATE layoff_copy2 
SET country=TRIM(country);

SELECT * 
FROM layoff_copy2;

/* Check whether the data entities can be organized / modified in a standard format  */

SELECT DISTINCT(company) 
FROM layoff_copy2 
ORDER BY 1;

/* REMOVING any Leading or Trailing spaces for the columns required ( eg--industry) */ 

UPDATE layoff_copy2 
SET industry=TRIM(industry);

/* Checking  whether the data entities can be organized / modified into a standard format 
   (eg--The industry named as 'Cryptocurrency' being set to a standard format "Crypto")*/
   
SELECT DISTINCT(industry)
FROM layoff_copy2 ORDER BY 1;

SELECT * 
FROM layoff_copy2
WHERE industry LIKE "Crypto%";

UPDATE layoff_copy2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT(country)
FROM layoff_copy2
ORDER BY 1;

UPDATE layoff_copy2
SET country=TRIM(TRAILING '.' FROM country); 

/* STANDARDIZIG OR MODIFYING THE DATA TYPES IF REQUIRED */

SELECT * 
FROM layoff_copy2;

DESC layoff_copy2;

/* CONVERTING THE 'DATE' COLUMN WHICH IS IN A TEXT TYPE TO DATE TYPE */

UPDATE layoff_copy2
SET `date`= STR_TO_DATE(`date`, "%m/%d/%Y");

ALTER TABLE layoff_copy2
MODIFY COLUMN `date` DATE;

DESC layoff_copy2;


/*  -------------------------------STEP 4--> NULL VALUE HANDLING AND REMOVAL OF UNNECESSARY COLUMNS AND ROWS: Trying to POPULATE the NULL Values if Possible-(Eg: Analyzing industry) ---------------------------  */

SELECT * 
FROM layoff_copy2
WHERE industry='' OR industry IS NULL;

SELECT *
FROM layoff_copy2
WHERE company IN ('Airbnb','Carvana','Juul');

SELECT l1.company,l1.industry, l2.industry,l2.company
FROM layoff_copy2 l1
JOIN layoff_copy2 l2
ON l1.company=l2.company
WHERE (l1.industry IS NULL OR l1.industry='')
AND l2.industry is NOT NULL;

SELECT l1.company,l1.industry,l1.`date`, l2.industry,l2.company,l2.`date`
FROM layoff_copy l1
JOIN layoff_copy l2
ON l1.company=l2.company
WHERE (l1.industry IS NULL OR l1.industry='')
AND l2.industry IS NOT NULL;

-- UPON EXECUTING THE BELOW 'UPDATE' QUERIES THE MISSING VALUES WERE POPULATED.                                         
UPDATE layoff_copy2
SET industry= NULL 
WHERE industry='';

UPDATE layoff_copy2 l1
JOIN layoff_copy2 l2
ON l1.company=l2.company
SET l1.industry=l2.industry
WHERE (l1.industry IS NULL OR l1.industry='')
AND l2.industry IS NOT NULL; 

SELECT l1.company,l1.industry, l2.industry,l2.company
FROM layoff_copy2 l1
JOIN layoff_copy2 l2
ON l1.company=l2.company
WHERE (l1.industry IS NULL OR l1.industry='')
AND l2.industry is NOT NULL;

SELECT company,industry
FROM layoff_copy2
WHERE company IN ('Airbnb','Carvana','Juul');

SELECT * from layoff_copy2;

/* Identify rows where both 'total_laid_off' and 'percentage_laid_off' are null */
SELECT *
FROM layoff_copy2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

/* Remove rows where both 'total_laid_off' and 'percentage_laid_off' are null */
DELETE 
FROM layoff_copy2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

/* Drop the 'row_num' column as it has served its purpose */
ALTER TABLE layoff_copy2
DROP COLUMN row_num;

-- THE TRANSFORMED, CLEAN, AND STRUCTURED DATA SET IS NOW SUITABLE FOR ANALYZING COMPANIES THAT CONDUCTED LAYOFFS DURING COVID.

SELECT *
FROM layoff_copy2;


/* ------------------------------------------------------EXPLORARTORY DATA ANALYSIS------------------------------------- */

SELECT * FROM layoff_copy2;

DESC layoff_copy2;

-- Total number of layoffs caused by the COVID pandemic.
SELECT  MIN(`date`) AS Start_Date ,MAX(`date`) AS End_DATE, SUM(total_laid_off) AS Total_Laid_off
FROM layoff_copy2;

-- Total number of Industries in our Data set 
SELECT COUNT(DISTINCT(industry)) AS Number_of_Industries
FROM layoff_copy2;

-- Total number of Countries in our Data set 
SELECT COUNT(DISTINCT(country)) AS Number_of_Countries
FROM layoff_copy2;

-- Query to retrieve the industries based on their layoff's.
SELECT industry,count(industry) AS Number_of_Companies,sum(total_laid_off) AS Total_laid_off
FROM layoff_copy2
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY 3 DESC;

-- Query to retrieve and rank the top 10 industries based on their layoff's.
SELECT *
FROM (SELECT *,
RANK() OVER (ORDER BY Total_laid_off DESC) AS Ranking
FROM (SELECT industry,SUM(total_laid_off) AS Total_laid_off
FROM layoff_copy2
GROUP BY industry) AS DT) 
AS DT2 WHERE Ranking <=10;

-- This query aims to rank the top 10 countries based on the total number of layoffs during the COVID-19 pandemic. 
-- It provides insights into how different countries were impacted by job cuts.

SELECT *
FROM (SELECT *,
RANK () OVER ( ORDER BY Total_laid_off DESC ) AS Ranking 
FROM (SELECT country,SUM(total_laid_off) AS Total_laid_off
FROM layoff_copy2
WHERE total_laid_off IS NOT NULL 
GROUP BY 1) AS DT) 
AS DT2 WHERE Ranking<=10;

-- This query aims to find the top 10 companies that laid off employees during the COVID-19 pandemic, broken down by year. 
-- It uses a rolling total to analyze year-wise layoffs for companies that had layoffs across multiple years.

SELECT *,
DENSE_RANK() OVER ( ORDER BY Rolling_Total DESC ) AS Ranking
FROM (SELECT company, On_Year, laid_off,
SUM(laid_off) OVER (PARTITION BY company ORDER BY company) AS Rolling_Total
FROM 
(SELECT company, YEAR(`date`) AS On_Year, SUM(total_laid_off) AS laid_off
FROM layoff_copy2
WHERE `date` AND total_laid_off IS NOT NULL
GROUP BY 1,2
ORDER BY 1) AS DT_SUM_Laid_off
)AS DT_Rolling_Total; 

-- This analysis focuses on the year-wise layoffs for companies that experienced layoffs across multiple years, 
-- Ranking the top 5 w.r.t to the number of employees laid off each year. 

WITH CTE_1
AS (SELECT company, YEAR(`date`) AS On_Year, SUM(total_laid_off) AS laid_off
FROM layoff_copy2
WHERE `date` AND total_laid_off IS NOT NULL
GROUP BY 1,2
ORDER BY 2), 
CTE_2 AS
(SELECT *,
DENSE_RANK() OVER (PARTITION BY On_Year ORDER BY laid_off DESC) AS Ranking
FROM CTE_1)
SELECT *
FROM CTE_2
WHERE RANKING<=5;

/* The same can be acheived using a Subquery */

SELECT * FROM
(WITH CTE_1
AS (SELECT company, YEAR(`date`) AS On_Year, SUM(total_laid_off) AS laid_off
FROM layoff_copy2
WHERE `date` AND total_laid_off IS NOT NULL
GROUP BY 1,2
ORDER BY 2)
SELECT *,
DENSE_RANK() OVER (PARTITION BY On_Year ORDER BY laid_off DESC) AS Ranking
FROM CTE_1) AS DT
WHERE Ranking<=5;


/* Analyze the ROLLING SUM of employee's laid off W.R.T Month-Year */

SELECT SUBSTRING_INDEX(`date`,"-",2) AS On_the_Year_of_Month, SUM(total_laid_off) AS Laid_off
FROM layoff_copy2
WHERE `date` IS NOT NULL 
GROUP BY 1
ORDER BY 1 DESC;

WITH Rolling_Total AS
(SELECT SUBSTRING_INDEX(`date`,"-",2) AS On_the_Year_of_Month, SUM(total_laid_off) AS Laid_off
FROM layoff_copy2
WHERE `date` IS NOT NULL 
GROUP BY 1
ORDER BY 1)
SELECT On_the_Year_of_Month, Laid_off,
SUM(Laid_off) OVER ( ORDER BY On_the_Year_of_Month) AS Rolling_Total_Laidoff
FROM Rolling_Total;


/* When we filter by date we notice the same company has laidoff employess at different dates, like Amazon..etc */
SELECT company, SUM(total_laid_off) AS laid_off,`date`
FROM layoff_copy2
WHERE total_laid_off IS NOT NULL
GROUP BY company,`date`
ORDER BY 1,2 DESC;


/*Some companies have been repeated muplitle times due to various factors such as the same company being from different location and various other factors.
  In order to analyze them we can make use of WINDOW FUNCTION */

SELECT company,`date`, laid_off,
SUM(laid_off) OVER (PARTITION BY company) AS Aggregated_laidoff
FROM (SELECT company, `date`, total_laid_off AS Laid_off
FROM layoff_copy2
WHERE total_laid_off IS NOT NULL
GROUP BY company,`date`,total_laid_off
ORDER BY 1,2 DESC)
AS Aggregated_laidoff;


/* Analyze the ROLLING SUM of employee's laid off W.R.T Month-Year */

SELECT SUBSTRING_INDEX(`date`,"-",2) AS Month_Year,SUM(total_laid_off) AS Laid_off
FROM layoff_copy2
WHERE  `date` AND total_laid_off IS NOT NULL 
GROUP BY 1
ORDER BY 1;

WITH Rolling_SUM AS 
(SELECT SUBSTRING_INDEX(`date`,"-",2) AS Month_Year,SUM(total_laid_off) AS Laid_off
FROM layoff_copy2
WHERE  `date` AND total_laid_off IS NOT NULL 
GROUP BY 1
ORDER BY 1)
SELECT *,
SUM(Laid_off) OVER (ORDER BY Month_Year) AS Rolling_Total
FROM Rolling_SUM;

/* The same can be acheived using a SUBQUERY as derived table */

SELECT Month_Year, laid_off,
SUM(laid_off) OVER (ORDER BY Month_Year) AS Rolling_Total
FROM (SELECT SUBSTRING_INDEX(`date`,"-",2) AS Month_Year, SUM(total_laid_off) AS laid_off
FROM layoff_copy2
WHERE total_laid_off AND `date` IS NOT NULL
GROUP BY 1
ORDER BY 1) AS DT; 

/* The the Rolling total off total_laid_off w.r.t company */


SELECT company, total_laid_off,
SUM(total_laid_off) OVER(ORDER BY company) AS Rolling_Total
FROM layoff_copy2
WHERE total_laid_off IS NOT NULL;

/* Analyze the ROLLING SUM of employee's laid off W.R.T Month-Year */

SELECT * FROM layoff_copy2;

WITH Rolling_Sum_CTE
AS (SELECT SUBSTRING_INDEX(`date`,"-",2) AS Month_Year, SUM(total_laid_off) AS laid_off
FROM layoff_copy2
WHERE total_laid_off AND `date` IS NOT NULL
GROUP BY 1
ORDER BY 1)
SELECT Month_Year,laid_off,
SUM(laid_off) OVER (ORDER BY Month_Year) AS Rolling_Total
FROM Rolling_Sum_CTE;







