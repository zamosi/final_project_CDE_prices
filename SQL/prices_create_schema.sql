CREATE SCHEMA RAW_DATA;

CREATE TABLE RAW_DATA.SNIFIM (
    storeid INT,
    bikoretno INT,
    storetype INT,
    storename VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    zipcode VARCHAR(20),
    file_name VARCHAR(255) NULL,
    num_reshet BIGINT NULL,
    file_date TIMESTAMP NULL,
    run_time TIMESTAMP NULL
);

CREATE TABLE RAW_DATA.PRICES (
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
    file_date TIMESTAMP NULL,
    run_time TIMESTAMP NULL
);

CREATE TABLE RAW_DATA.PRICES_N (
    priceupdatedate VARCHAR(100),
    itemcode VARCHAR(100),
    itemtype VARCHAR(100),
    itemname VARCHAR(255),
    manufacturername VARCHAR(100),
    manufacturecountry VARCHAR(100),
    manufactureritemdescription VARCHAR(255),
    unitqty VARCHAR(50),
    quantity VARCHAR(100),
    unitofmeasure VARCHAR(50),
    bisweighted VARCHAR(100),
    qtyinpackage VARCHAR(100),
    itemprice VARCHAR(100),
    unitofmeasureprice VARCHAR(100),
    allowdiscount VARCHAR(100),
    itemstatus VARCHAR(100),
    itemid VARCHAR(100),
    file_name VARCHAR(255),
    num_reshet VARCHAR(100),
    num_snif VARCHAR(100),
    file_date TIMESTAMP,
    run_time TIMESTAMP
);


CREATE TABLE RAW_DATA.RESHATOT (
    reshet_name VARCHAR(255),
    reshet_num BIGINT,
    vendor_website VARCHAR(100),
    user_name VARCHAR(100),
    password VARCHAR(255),
    run_time TIMESTAMP
);


CREATE SCHEMA dwh;

CREATE TABLE dwh.prices_scd (
    itemcode BIGINT,
    itemname VARCHAR(255),
    itemprice FLOAT,
    StartDate DATE,
    EndDate DATE,
    IsActive INT,
    reshet_num BIGINT,
    snif_num INT
);


CREATE TABLE dwh.prices_data (
    snapshot DATE,
    snapshot_month VARCHAR(10),
    snapshot_quarter VARCHAR(10),
    is_weekend BOOLEAN,
    itemcode BIGINT,
    itemname VARCHAR(100),
    manufacturername VARCHAR(50),
    manufacturecountry VARCHAR(20),
    manufactureritemdescription VARCHAR(100),
    unitqty VARCHAR(30),
    unitofmeasure VARCHAR(25),
    qtyinpackage VARCHAR(7),
    itemprice FLOAT,
    unitofmeasureprice FLOAT,
    allowdiscount BOOLEAN,
    file_name VARCHAR(50),
    num_reshet VARCHAR(13),
    priceupdatedate TIMESTAMP,
    file_date TIMESTAMP,
    run_time TIMESTAMP,
    num_snif INTEGER,
    reshet_name VARCHAR(50),
    storename VARCHAR(50),
    address VARCHAR(30),
    city VARCHAR(30),
    zipcode INTEGER,
    days_since_last_price_update INTEGER,
    is_price_update_stale INTEGER
) PARTITION BY RANGE (snapshot);