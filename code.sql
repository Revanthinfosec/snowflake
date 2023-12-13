create stage my_stage
URL = "s3://snowflakedb-app/mydata/"
CREDENTIALS = (AWS_KEY_ID = 'Give_yours_aws_access_key' AWS_SECRET_KEY = 'give_yours_aws_secret_key');

CREATE OR REPLACE FILE FORMAT csv_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY='"';



//created the aisles table
CREATE OR REPLACE TABLE aisles (
aisle_id INTEGER PRIMARY KEY,
aisle VARCHAR
);
///copied the data from aws to our snowflake
COPY INTO aisles (aisle_id, aisle)
FROM @my_stage/aisles.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');



//departments
CREATE OR REPLACE TABLE departments (
department_id INTEGER PRIMARY KEY,
department VARCHAR
);
COPY INTO departments (department_id, department)
FROM @my_stage/departments.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');



//products
CREATE OR REPLACE TABLE products (
product_id INTEGER PRIMARY KEY,
product_name VARCHAR,
aisle_id INTEGER,
department_id INTEGER
);
COPY INTO products (product_id, product_name, aisle_id, department_id)
FROM @my_stage/products.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');


//orders table
CREATE OR REPLACE TABLE orders (
order_id INTEGER PRIMARY KEY,
user_id INTEGER,
eval_set STRING,
order_number INTEGER,
order_dow INTEGER,
order_hour_of_day INTEGER,
days_since_prior_order INTEGER
);
COPY INTO orders (order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order)
FROM @my_stage/orders.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');


//order order_products
CREATE OR REPLACE TABLE order_products (
order_id INTEGER,
product_id INTEGER,
add_to_cart_order INTEGER,
reordered INTEGER,
PRIMARY KEY (order_id, product_id)
);
COPY INTO order_products (order_id, product_id, add_to_cart_order, reordered)
FROM @my_stage/order_products.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');



//transformation phase such as constructing the facts, dim table
CREATE OR REPLACE TABLE dim_users AS (
SELECT user_id FROM orders
);

CREATE OR REPLACE TABLE dim_products AS (
SELECT product_id, product_name FROM products
);

CREATE OR REPLACE TABLE dim_aisles AS (
SELECT aisle_id, aisle FROM aisles
);

CREATE OR REPLACE TABLE dim_departments AS (
SELECT department_id, department FROM departments
);

CREATE OR REPLACE TABLE dim_orders AS (
SELECT order_id, order_number, order_dow, order_hour_of_day, days_since_prior_order FROM orders
);

//building the fact table
CREATE OR REPLACE TABLE fact_order_product AS (
SELECT 
op.order_id, 
op.product_id, 
o.user_id,
p.department_id,
p.aisle_id,
op.add_to_cart_order,
op.reordered
FROM 
order_products op
JOIN 
    orders o ON op.order_id = o.order_id
    JOIN 
        products p ON op.product_id = p.product_id
);


//now analytic part
SELECT d.department, COUNT(*) AS total_products_ordered
FROM 
fact_order_product fop
JOIN
dim_departments d ON fop.department_id = d.department_id
GROUP BY
d.department;

--to find the top 5 aisles with high sell of reordered products
SELECT a.aisle, COUNT(*) AS total_reordered FROM fact_order_product fop JOIN dim_aisles a ON fop.aisle_id = a.aisle_id WHERE fop.reordered = TRUE GROUP BY a.aisle ORDER BY total_reordered DESC LIMIT 5;


