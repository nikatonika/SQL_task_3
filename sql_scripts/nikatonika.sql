-- Создать таблицы со следующими структурами и загрузить данные из csv-файлов
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(50),
    DOB VARCHAR(50),
    job_title VARCHAR(255),
    job_industry_category VARCHAR(255),
    wealth_segment VARCHAR(255),
    deceased_indicator CHAR(1),
    owns_car CHAR(50),
    address VARCHAR(255),
    postcode INT,
    state VARCHAR(50), 
    country VARCHAR(100), 
    property_valuation INT
);

-- ![customer_table_data_import](screenshots/screenshot_1.png)

COPY customers
FROM 'https://github.com/nikatonika/SQL_task_3/data/customer.csv'
DELIMITER ';' CSV HEADER;

CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    transaction_date VARCHAR(50),  
    online_order BOOLEAN,
    order_status VARCHAR(50), 
    brand VARCHAR(255),
    product_line VARCHAR(255),
    product_class VARCHAR(255),
    product_size VARCHAR(255),
    list_price DECIMAL(10, 2),
    standard_cost DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- ![transaction_table_structure](screenshots/screenshot_2.png)

-- ![database_structure](screenshots/screenshot_3.png)

COPY transactions
FROM 'https://github.com/nikatonika/SQL_task_3/data/transaction.csv'
DELIMITER ',' CSV HEADER;

-- ![data_import](screenshots/screenshot_4.png)

-- Подготовка данных

SELECT transaction_date
FROM transactions
WHERE transaction_date !~ '^\d{2}\.\d{2}\.\d{4}$';

UPDATE transactions
SET transaction_date = CAST(transaction_date AS DATE);

ALTER TABLE transactions 
ALTER COLUMN transaction_date TYPE DATE
USING TO_DATE(transaction_date,'DD.MM.YYYY');

-- ![data_normalization](screenshots/screenshot_5.png)

-- Выполнить следующие запросы:

-- Задача 1: Вывести распределение (количество) клиентов по сферам деятельности, отсортировав результат по убыванию количества.
SELECT job_industry_category, COUNT(*) AS number_of_clients
FROM customers
GROUP BY job_industry_category
ORDER BY number_of_clients DESC;

-- ![number_of_customers_by_job_industry_category](screenshots/1.png)

-- Задача 2: Найти сумму транзакций за каждый месяц по сферам деятельности, отсортировав по месяцам и по сфере деятельности.
SELECT 
    TO_CHAR(t.transaction_date, 'MM.YYYY') AS transaction_month,
    c.job_industry_category,
    SUM(t.list_price) AS total_sales
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
GROUP BY transaction_month, c.job_industry_category
ORDER BY transaction_month, c.job_industry_category;

-- !monthly_online_transactions_by_job_industry_category](screenshots/2.png)

-- Задача 3: Вывести количество онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT.

SELECT 
    brand,
	COUNT(*) AS online_orders_count, 
    job_industry_category
FROM transactions
JOIN customers ON transactions.customer_id = customers.customer_id
WHERE online_order = 'True'
AND order_status = 'Approved'
AND job_industry_category = 'IT'
GROUP BY job_industry_category, brand
ORDER BY online_orders_count DESC, job_industry_category, brand;

-- ![approved_online_transactions_from_IT](screenshots/3.png)

-- Задача 4: Найти по всем клиентам сумму всех транзакций (list_price), максимум, минимум и количество транзакций,
-- отсортировав результат по убыванию суммы транзакций и количества клиентов. 
-- Выполните двумя способами: используя только group by и используя только оконные функции. Сравните результат.

-- Вывод с помощью group by:

SELECT customer_id,
       SUM(list_price) AS total_sales,
       MAX(list_price) AS max_sale,
       MIN(list_price) AS min_sale,
       COUNT(*) AS transactions_count
FROM transactions
GROUP BY customer_id
ORDER BY total_sales DESC, transactions_count DESC;

-- ![sum_of_all_transactions_by_group_by](screenshots/41.png)

-- Вывод с оконных функций:

SELECT COUNT(DISTINCT customer_id) AS total_rows_window_function
FROM (
    SELECT customer_id,
           SUM(list_price) OVER (PARTITION BY customer_id) AS total_sales,
           MAX(list_price) OVER (PARTITION BY customer_id) AS max_sale,
           MIN(list_price) OVER (PARTITION BY customer_id) AS min_sale,
           COUNT(*) OVER (PARTITION BY customer_id) AS transactions_count
    FROM transactions
) AS subquery;

-- Дополнительная проверка:

SELECT COUNT(*) AS total_rows_group_by
FROM (
    SELECT customer_id
    FROM transactions
    GROUP BY customer_id
) AS subquery;
-- ![total_rows_by_group_by](screenshots/43.png)

SELECT COUNT(DISTINCT customer_id) AS total_rows_window_function
FROM (
    SELECT customer_id,
           SUM(list_price) OVER (PARTITION BY customer_id) AS total_sales,
           MAX(list_price) OVER (PARTITION BY customer_id) AS max_sale,
           MIN(list_price) OVER (PARTITION BY customer_id) AS min_sale,
           COUNT(*) OVER (PARTITION BY customer_id) AS transactions_count
    FROM transactions
) AS subquery;
-- ![total_rows_by_window_functions](screenshots/44.png)

-- Я УБЕДИЛАСЬ, ЧТО РЕЗУЛЬТАТ ПРИ ВЫВОДЕ С ПОМОЩЬЮ GROUP BY И С ПОМОЩЬЮ ОКОННЫХ ФУНКЦИЙ ОДИНАКОВЫЙ.

-- Задача 5: Вывести все бренды, которые закупают клиенты, работающие в сфере Financial Services.
SELECT DISTINCT t.brand
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
WHERE c.job_industry_category = 'Financial Services';

-- ![brands_by_FinancialServices_customers](screenshots/screenshot_9.png)

-- Запрос 5: Найти имена и фамилии клиентов с минимальной/максимальной суммой транзакций за весь период 
-- (сумма транзакций не может быть null). Напишите отдельные запросы для минимальной и максимальной суммы.

-- Клиент с максимальной суммой покупок

SELECT c.first_name, c.last_name, max_sales.total_sales
FROM customers c
JOIN (
    SELECT customer_id, SUM(list_price) AS total_sales
    FROM transactions
    WHERE list_price IS NOT NULL
    GROUP BY customer_id
    ORDER BY total_sales DESC
    LIMIT 1
) AS max_sales ON c.customer_id = max_sales.customer_id;

-- ![client_max_sales](screenshots/51.png)

-- Клиент с минимальной суммой покупок

SELECT c.first_name, c.last_name, min_sales.total_sales
FROM customers c
JOIN (
    SELECT customer_id, SUM(list_price) AS total_sales
    FROM transactions
    WHERE list_price IS NOT NULL
    GROUP BY customer_id
    ORDER BY total_sales ASC
    LIMIT 1
) AS min_sales ON c.customer_id = min_sales.customer_id;

-- ![client_min_sales](screenshots/52.png)

-- Топ 10 клиентов с максимальной суммой покупок

SELECT c.first_name, c.last_name, max_sales.total_sales
FROM customers c
JOIN (
    SELECT customer_id, SUM(list_price) AS total_sales
    FROM transactions
    WHERE list_price IS NOT NULL
    GROUP BY customer_id
    ORDER BY total_sales DESC
    LIMIT 10
) AS max_sales ON c.customer_id = max_sales.customer_id
ORDER BY max_sales.total_sales DESC;

-- ![top10_clients_max_sales](screenshots/53.png)

-- Топ 10 клиентов с минимальной суммой покупок

SELECT c.first_name, c.last_name, min_sales.total_sales
FROM customers c
JOIN (
    SELECT customer_id, SUM(list_price) AS total_sales
    FROM transactions
    WHERE list_price IS NOT NULL
    GROUP BY customer_id
    ORDER BY total_sales ASC
    LIMIT 10
) AS min_sales ON c.customer_id = min_sales.customer_id
ORDER BY min_sales.total_sales ASC;

-- ![top10_clients_min_sales](screenshots/54.png)



-- Задача 6: Вывести только самые первые транзакции клиентов. Решить с помощью оконных функций. 

SELECT DISTINCT
    first_value(customer_id) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS customer_id,
    first_value(transaction_id) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS transaction_id,
    first_value(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS first_transaction_date
FROM transactions;

-- ![first_customer_transactions](screenshots/6.png)

-- Задача 7: Вывести имена, фамилии и профессии клиентов, между транзакциями которых был максимальный интервал (интервал вычисляется в днях) 

WITH RankedTransactions AS (
    SELECT 
        customer_id, 
        transaction_date,
        LAG(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS previous_transaction_date
    FROM transactions
),
DatedTransactions AS (
    SELECT 
        customer_id, 
        transaction_date, 
        previous_transaction_date,
        transaction_date - previous_transaction_date AS days_interval
    FROM RankedTransactions
    WHERE previous_transaction_date IS NOT NULL
),
MaxIntervalCustomer AS (
    SELECT 
        customer_id, 
        MAX(days_interval) AS max_days_interval
    FROM DatedTransactions
    GROUP BY customer_id
)
SELECT 
    c.first_name,
    c.last_name,
    c.job_title,
    mic.max_days_interval
FROM customers c
JOIN MaxIntervalCustomer mic ON c.customer_id = mic.customer_id
ORDER BY mic.max_days_interval DESC;

-- ![customers_with_max_transactions_interval](screenshots/7.png)
