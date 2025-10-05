-- EXPLORATORY Data Analysis  

select *
from layoffs_staging2;

-- ________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________

SELECT MAX(percentage_laid_off), MIN(percentage_laid_off), AVG(percentage_laid_off)
FROM layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1;


select company, SUM(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


SELECT MIN(`date`) , max(`date`)
from layoffs_staging2;

select industry, SUM(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;


select country, SUM(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;



   -- year basis ----------

select year(`date`), SUM(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc;



-- percantage -------------------------------------------
select company, AVG(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;




select substring(`date`,6,2) as "MONTH" , SUM(total_laid_off)
from layoffs_staging2
group by 1;


select substring(`date`,1,7) as "MONTH" , SUM(total_laid_off)
from layoffs_staging2
group by 1
order by 1 desc;


-- cumulative in each month 
with rolling_total as(

select substring(`date`,1,7) as `MONTH` , SUM(total_laid_off) as total_off
from layoffs_staging2
group by `MONTH`
order by 1 asc )

select `MONTH` , total_off,
SUM(total_off)  over(order by `MONTH`) as rolling_total
from rolling_total;



-- company and thery total_laid_off
select company, year(`date`), SUM(total_laid_off)
from layoffs_staging2
group by company, 2
order by 3 desc;

-- -----------------------------------------------------------------

WITH Company_year (company, years, total_laid_off) AS (
    SELECT company, YEAR(`date`), SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, 2
)
SELECT *, DENSE_RANK() 
       OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS RANKING    -- Rank companies within each year by total layoffs (highest = 1)
FROM Company_year
ORDER BY RANKING;   -- Order by ranking so that rank 1 of each year appears first




-- we want to filter onn ranking and get to 5 company in each year ( 2 ctes)

WITH Company_year (company, years, total_laid_off) AS (
    SELECT company, YEAR(`date`), SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, 2
) ,
 Company_year_Rank as(
SELECT *, DENSE_RANK() 
       OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS RANKING    
FROM Company_year
)
SELECT *
from Company_year_Rank
WHERE RANKING <= 5;





