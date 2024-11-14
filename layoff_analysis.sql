use world_layoffs;
select * from layoffs_staging2;
-- seleting max layoffs

select max(total_laid_off), max(percentage_laid_off) from layoffs_staging2;

-- selecting 100% layoff companies( 1 = 100%)
select * from layoffs_staging2 where percentage_laid_off =1;

select * from layoffs_staging2
where percentage_laid_off =1
order by funds_raised_millions desc
;

select company, country, sum(total_laid_off) from layoffs_staging2
group by company,country order by 3 desc;

select min(date), max(date) from layoffs_staging2;
-- industry wise layoffs
select company, industry, sum(total_laid_off) from layoffs_staging2
group by company,industry order by 3 desc;
-- country
select country, sum(total_laid_off) from layoffs_staging2
group by country order by 2 desc;
-- by year
select year(date), sum(total_laid_off) from layoffs_staging2
group by year(date) order by 1 desc;

-- by months
select substring(date,6,2) as Months, sum(total_laid_off) from layoffs_staging2
group by Months order by 2 desc;

-- stage of company
select stage, sum(total_laid_off) from layoffs_staging2
group by stage order by 2 desc;

-- rolling total according to dates
with rolling_total as
(
	select substring(date,1,7) as Months, sum(total_laid_off) as Total_Layoffs 
    from layoffs_staging2 where substring(date,1,7) is not null
	group by Months order by 1
)
select Months, Total_Layoffs, sum(Total_Layoffs) over(order by Months) as Rolling_Total
from rolling_total;

-- company with year
select company, year(date), sum(total_laid_off) from layoffs_staging2
group by company,year(date) order by 3 desc;

-- Ranking with year w.r.t compaies
with company_year(company, years, total_laid_off) as
(
select company, year(date), sum(total_laid_off) from layoffs_staging2
group by company,year(date)
), company_year_ranking as
(
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year where years is not null
)
select * from company_year_ranking
where ranking <=5
;