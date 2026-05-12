-- create the coffee_sales table in Redshift
CREATE TABLE IF NOT EXISTS coffee_sales (
    sale_date       DATE,
    sale_datetime   TIMESTAMP,
    cash_type       VARCHAR(50),
    card            VARCHAR(100),
    amount          FLOAT,
    coffee_name     VARCHAR(100)
);

-- grant permissions
GRANT ALL ON TABLE coffee_sales TO PUBLIC;