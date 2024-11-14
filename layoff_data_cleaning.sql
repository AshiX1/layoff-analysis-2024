-- Data Cleaning --

-- 1. COPYNG DATA FROM MASTER TABLE
CREATE TABLE LAYOFFS_STAGING LIKE LAYOFFS;
INSERT LAYOFFS_STAGING SELECT * FROM LAYOFFS;

-- REMOVING DUPLICATES
SELECT * FROM LAYOFFS_STAGING;
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
from layoffs_staging;

/*WITH duplicate_cte as(
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

SELECT * FROM layoffs_staging WHERE company = 'Oda'; 
-- from above partition wehave seen that it;s not givig exact duplicate values so we have to partition all the column
*/
WITH duplicate_cte AS(
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num >1;

SELECT * FROM layoffs_staging WHERE company = 'Casper';
-- creating new stagging table to add row_num colmun so we can delete the duplicate based on row number
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
  row_num int
)
 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
from layoffs_staging;
SELECT * FROM layoffs_staging2;
DELETE FROM layoffs_staging2 WHERE row_num >1;
SELECT * FROM layoffs_staging2 WHERE row_num >1; -- NO DATA
-- STANDARDIZING DATA
UPDATE layoffs_staging2 SET company = TRIM(company); -- removing white spaces

-- select industry, trim(industry) from layoffs_staging2;
UPDATE layoffs_staging2 SET industry = TRIM(industry);

SELECT DISTINCT industry FROM layoffs_staging2;

UPDATE layoffs_staging2 SET industry ='Crypto'
WHERE industry LIKE 'Crypto%'; -- updated all crypto type company to one

UPDATE layoffs_staging2 SET country ='United States'
WHERE country LIKE 'Crypto%';
SELECT DISTINCT country FROM layoffs_staging2 order by 1;
-- or
/*SELECT DISTINCT country, TRIM(TRAILING'.' FROM country)
FROM layoffs_staging2 ORDER BY 1; --TRAILING IS USED TO FETCH end */

-- Formating Date-Time
SELECT date FROM layoffs_staging2;
UPDATE layoffs_staging2 SET date =  STR_TO_DATE(date,'%m/%d/%Y');
ALTER TABLE layoffs_staging2 MODIFY COLUMN date DATE;

-- NULL AND BLANK VALUE

SELECT company, industry FROM layoffs_staging2
WHERE industry IS NULL OR industry ='';
SELECT * FROM layoffs_staging2
WHERE company LIKE 'Airbnb%';
-- Now we have to fill those blank or nul values of industries with respective companies
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL WHERE industry ='';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%'; -- it;s single

SELECT * FROM layoffs_staging2 -- These data have no layoffs so we can delete them
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL;

DELETE FROM layoffs_staging2
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL; -- DELETED
 ALTER TABLE layoffs_staging2 
 DROP COLUMN row_num; -- Now we don't need it