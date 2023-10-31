select * from customer_data limit 5;

-- let's see what is structure of table
describe customer_data;

-- lets get distinct values from categorical columns
select distinct gender ,count(gender) as total_num from customer_data group by gender;
select distinct country,count(country) as total_num from customer_data group by country;

-- let's get mean ,mode ,max ,sum ,min
-- this mysql does not have median function in-built , we'll do it using postgresql
SELECT
    'credit_score' AS numeric_column_name,
    AVG(credit_score) AS mean,
    (SELECT credit_score FROM customer_data GROUP BY credit_score ORDER BY COUNT(*) DESC LIMIT 1) AS mode,
    STDDEV(credit_score) AS sd,
    MAX(credit_score) AS max,
    MIN(credit_score) AS min
FROM customer_data
UNION ALL
SELECT
    'age' AS numeric_column_name,
    AVG(age) AS mean,
    (SELECT age FROM customer_data GROUP BY age ORDER BY COUNT(*) DESC LIMIT 1) AS mode,
    STDDEV(age) AS sd,
    MAX(age) AS max,
    MIN(age) AS min
FROM customer_data
UNION ALL
SELECT
    'balance' AS numeric_column_name,
    AVG(balance) AS mean,
    (SELECT balance FROM customer_data GROUP BY balance ORDER BY COUNT(*) DESC LIMIT 1) AS mode,
    STDDEV(balance) AS sd,
    MAX(balance) AS max,
    MIN(balance) AS min
FROM customer_data
UNION ALL
SELECT
    'tenure' AS numeric_column_name,
    AVG(tenure) AS mean,
    (SELECT tenure FROM customer_data GROUP BY tenure ORDER BY COUNT(*) DESC LIMIT 1) AS mode,
    STDDEV(tenure) AS sd,
    MAX(tenure) AS max,
    MIN(tenure) AS min
FROM customer_data
UNION ALL
SELECT
    'estimated_salary' AS numeric_column_name,
    AVG(estimated_salary) AS mean,
    (SELECT estimated_salary FROM customer_data GROUP BY estimated_salary ORDER BY COUNT(*) DESC LIMIT 1) AS mode,
    STDDEV(estimated_salary) AS sd,
    MAX(estimated_salary) AS max,
    MIN(estimated_salary) AS min
FROM customer_data;


-- lets explore the distribution of country and credit-score with each other
SELECT
    CASE
        WHEN credit_score BETWEEN 100 AND 200 THEN '100-200'
        WHEN credit_score BETWEEN 200 AND 300 THEN '200-300'
				WHEN credit_score BETWEEN 300 AND 400 THEN '300-400'
				WHEN credit_score BETWEEN 400 AND 500 THEN '400-500'
				WHEN credit_score BETWEEN 500 AND 600 THEN '500-600'
				WHEN credit_score BETWEEN 600 AND 700 THEN '600-700'
				WHEN credit_score BETWEEN 700 AND 800 THEN '700-800'
				WHEN credit_score BETWEEN 800 AND 900 THEN '800-900'
				WHEN credit_score BETWEEN 900 AND 1000 THEN '900-1000'
        ELSE 'Other' 
    END AS credit_score_range,
    country,
    COUNT(*) AS count_all
FROM customer_data
GROUP BY credit_score_range ,country
order by country,count_all desc;


-- lets look the relationship of churn with respect to extimated salary, balance and gender
-- The value 0 represents the customer did not churn meaning customer stayed and value 1 represents customer left of churned. 
SELECT
    gender,
    AVG(estimated_salary) AS average_salary,
    AVG(balance) AS average_bal,
    case
		WHEN churn = 0 THEN 'stayed'
        WHEN churn = 1 THEN 'not-stayed'
	end as churn_status ,
    COUNT(*) AS total_customers
FROM customer_data
GROUP BY gender,churn_status;


-- lets look into churn ,credit card and age relationship
-- the age has be converted from continuos data to data range for better analysis
SELECT 
    CASE
        WHEN age BETWEEN 10 AND 20 THEN '10-20'
        WHEN age BETWEEN 20 AND 30 THEN '20-30'
				WHEN age BETWEEN 30 AND 40 THEN '30-40'
				WHEN age BETWEEN 40 AND 50 THEN '40-50'
				WHEN age BETWEEN 50 AND 60 THEN '50-60'
				WHEN age BETWEEN 60 AND 70 THEN '60-70'
				WHEN age BETWEEN 70 AND 80 THEN '70-80'
				WHEN age BETWEEN 80 AND 90 THEN '80-90'
				WHEN age BETWEEN 90 AND 100 THEN '90-100'
        -- Add more ranges as needed
        ELSE 'Other'  -- Handle any scores outside the specified ranges
    END AS age_range,
    churn,credit_card,
    COUNT(*) AS frequency
FROM customer_data
GROUP BY age_range,churn,credit_card
ORDER BY churn,credit_card,frequency desc;


-- lets look at active members in each country
select 
active_member,
churn,
country,
count(*) as total_number 
from customer_data 
group by active_member,country ,churn
order by country,total_number desc;



-- lets look at the customers with higher balance to salary ratio,mostly top 5
select * from (
	select 
	customer_id,credit_score , churn,
	(balance/estimated_salary) as balance_to_salary_ratio 
	from customer_data 
	where credit_score>600 and balance > 0 
	order by credit_score desc) t 
where t.balance_to_salary_ratio > 10 
order by t.balance_to_salary_ratio desc 
limit 5;

-- lets look at the customers with lower balance to salary ratio,mostly top 5 
select * from (
	select 
	customer_id,credit_score , churn,
	(balance/estimated_salary) as balance_to_salary_ratio 
	from customer_data 
	where credit_score>600 and balance > 0 
	order by credit_score asc) t 
order by t.balance_to_salary_ratio asc 
limit 5;
