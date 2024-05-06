-- Data Cleaning

SELECT *
FROM layoffs;

-- Create new table layoffs_staging to manipulate our data in
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- Copy the raw data to layoff_staging
INSERT layoffs_staging
SELECT *
FROM layoffs;


SELECT *
FROM layoffs_staging;

-- 1. Removing duplicates 
-- without a unique ID column we need to find a way to see if there are any duplicates by creating our own column
-- Find the number of rows which have the same information for company, industry, total_laid_off, etc. The row_num will indicate how many rows there are with that info

SELECT *,
row_number() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Now we need to make a CTE to use the value "row_num" and select rows which have a row number of > 1 as this indicates that they are duplicates

WITH duplicate_cte AS 
(
SELECT *,
row_number() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- Now we need to delete duplicates from the table
-- Will have to create a table that has the extra row "row_num" and then delete values where that row is equal to or greater than 2

-- Right click on "layoffs_staging" > Copy to clipboard > Create statement   Then change 'layoffs_staging' to 'layoffs_staging2' and add a row labeled 'row-num' with data type as INT ( `row_num` INT)

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;
-- Now layoffs_staging2 is a table but it does not have any data in it

-- To add data to the table we take all the rows and partition by all values as row_num to get the row_num information (number of duplicate rows). This is important as it is the whole reason we made another table
INSERT INTO layoffs_staging2
SELECT *,
row_number() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;
-- Now we have all the data with the row_num as a part of the table and we can filter for values greater than 1 to see duplicate values

SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- All duplicates successfully removed


-- 2. Standardizing data 
-- To remove the space before some of the company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
Set company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

-- Some values under industry are not standardized. There are values for 'Crypto' and 'Crypto Currency' which should be the same

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Now all vaues that started with 'Crypto' are now changed to Crypto and everything is standardized

-- Fixing values in location

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE location LIKE '%sseldorf';

UPDATE layoffs_staging2
SET location = 'Dusseldorf'
WHERE location LIKE '%sseldorf';

SELECT *
FROM layoffs_staging2
WHERE location LIKE 'Florian%';

UPDATE layoffs_staging2
SET location = 'Florianopolis'
WHERE location LIKE 'FlorianÃ³polis';

SELECT *
FROM layoffs_staging2
WHERE location LIKE 'Malm%';

UPDATE layoffs_staging2
SET location = 'Malmo'
WHERE location LIKE 'Malm%';

-- Fixing values in country

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Currently the date is in the form of text which will not work if we want to do time series analysis. (If look under schemas>columns>date it is a text column)
-- Need to change to a date column

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- Do not do things like this on the raw table because we are changing the data type of the table
-- Changing the date column from text to date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null values or blank values
-- Now we have to deal with the NULL values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

-- Airbnb had a blank for industry. Lets look at the other Airbnb columns to see if we can populate it.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Need to do a join statement for this. We want to update the blank with the non blank from the other Airbnb row

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL OR t1.industry = ''
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- This one did not fill in the industry because it is the only row for the company "Bally's Interactive." It had nothing to populate the empty row with.


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- These rows all have no data for total_laid_off AND percentage_laid_off which could lead us to believe that they did not lay off anyone so we will delete this information

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

-- 4. Remove any columns or rows
-- Now we want to drop the column row_num because we only needed it to remove duplicate values
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;







