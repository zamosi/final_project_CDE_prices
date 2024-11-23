-- Create the schema
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Create the snifim table
CREATE TABLE IF NOT EXISTS raw_data.snifim (
    storeid INT,
    bikoretno INT,
    storetype INT,
    storename VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    zipcode VARCHAR(20),
    file_name VARCHAR(255) NULL,
    num_reshet BIGINT NULL,
    num_snif INT NULL,
    file_date TIMESTAMP NULL
);

-- Create the prices table
CREATE TABLE IF NOT EXISTS raw_data.prices (
    priceupdatedate TIMESTAMP NULL,
    itemcode BIGINT NULL,
    itemtype INT NULL,
    itemname VARCHAR(255) NULL,
    manufacturername VARCHAR(100) NULL,
    manufacturecountry VARCHAR(100) NULL,
    manufactureritemdescription VARCHAR(255) NULL,
    unitqty VARCHAR(50) NULL,
    quantity FLOAT NULL,
    unitofmeasure VARCHAR(50) NULL,
    bisweighted INT NULL,
    qtyinpackage VARCHAR(100) NULL,
    itemprice FLOAT NULL,
    unitofmeasureprice FLOAT NULL,
    allowdiscount INT NULL,
    itemstatus INT NULL,
    itemid INT NULL,
    file_name VARCHAR(255) NULL,
    num_reshet BIGINT NULL,
    num_snif INT NULL,
    file_date TIMESTAMP NULL
);
