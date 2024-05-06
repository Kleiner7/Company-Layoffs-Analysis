-- Exploratory Data Analysis 

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Determining date range of this data 
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;
-- 2020-03-11 to 2023-03-06

-- Which industries had the most layoffs? Least Layoffs?
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
-- Top three industries for number of layoff: Consumer, Retail, Other. These make sense considering the time period (COVID)
-- Bottom three industries for number of layoffs: Manufacturing, Fin-Tech, Aerospace

-- Which countries had the most layoffs?
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
-- Top three countries for number of layoffs: United States, India, Netherlands

-- Which years had the most layoffs?
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
-- 2022 had the most layoffs

-- How did the number of layoffs differ by stage?
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Number of layoffs by month
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Find the rolling sum of layoffs by month
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) as total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off
,SUM(total_off) OVER(ORDER BY `MONTH`) AS running_total
FROM Rolling_Total;


-- Look at how many layoffs each company had by year
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

-- If we want to rank which year each company let go of more people. Year with most people would be ranked 1
-- Need to make CTE's

WITH Company_Year (company,years,total_laid_off) AS  -- Creating a CTE with the company and year with the total lay offs for each year
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC
), Company_Year_Rank AS  -- Creating a CTE with the rank then below we can filter by the rank
(SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5
;

-- Look at number of layoffs by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC;
-- The top three locations with the highest number of layoffs: SF Bay Area, Seattle, New York City

-- Look at number of layoffs by industry
-- Layoffs per year within Industry 
SELECT industry, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry, YEAR(`date`)
ORDER BY 1 DESC;

-- Ranking which year each industry let go of the most people
WITH Industry_Year (industry, years, total_laid_off) AS (
	SELECT industry, YEAR(`date`), SUM(total_laid_off)
    FROM layoffs_staging2
	WHERE industry IS NOT NULL
    GROUP BY industry, YEAR(`date`)
    ORDER BY industry
    ),
Industry_Year_Rank AS (
SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Industry_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Industry_Year_Rank
WHERE Ranking <= 5;
-- Shows which industries had the most layoffs by year. Ranking of 1 represents the highest number of layoffs that year.

