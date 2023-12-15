//creation of schema and databases
create or replace database manage_db;
create or replace schema external_stages;

//creation of aws_stage with credentials
create or replace stage manage_db.external_stages.aws_stage
url='s3://snowflakedb-app/mydata/'
credentials=(aws_key_id='AKIAQPONNB4QLLXKJNVE' aws_secret_key='b+n2WXI/pkYmXK5VrylyOXkDJV6RNP2I1o2vexZ7');


//to get a description of stage external stage
desc stage manage_db.external_stages.aws_stage;

//now lets list all
list @aws_stage;

//now lets load the data using copy command
create or replace table manage_db.public.orders (
ORDER_ID VARCHAR(30),
AMOUNT INT,
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30)
);
//SELECT ALL FROM ABOVE TO VERIFY
select * from manage_db.public.orders;

//lets copy/load the data from our s3 bucket and load it into a new table such as orders
copy into manage_db.public.orders
from @aws_stage
file_format = (type = csv field_delimiter="," skip_header=1)
files=('OrderDetails.csv')

//now we can do the data alter such as editing
create or replace table manage_db.public.orders_clone (
ORDER_ID VARCHAR(30),
AMOUNT INT
);
copy into manage_db.public.orders_clone
from (select s.$1, s.$2 from @manage_db.external_stages.aws_stage s)
file_format = (type = csv field_delimiter="," skip_header=1)
files=('OrderDetails.csv')
select * from manage_db.public.orders_clone;

//cloning again
create or replace table manage_db.public.orders_clone2 (
ORDER_ID VARCHAR(30),
AMOUNT INT,
PROFIT INT,
CATEGORY_SUBSTRING VARCHAR(255)
);
copy into manage_db.public.orders_clone2
from (select s.$1, s.$2, s.$3,
case when cast(s.$3 as int) < 0 then 'not profitable' else 'profitable' END
from @manage_db.external_stages.aws_stage s)
file_format = (type = csv field_delimiter="," skip_header=1)
files=('OrderDetails.csv')
select * from manage_db.public.orders_clone2;


