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
    num_snif INT NULL,
    file_date TIMESTAMP NULL
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
    file_date TIMESTAMP NULL
);

CREATE TABLE RAW_DATA.RESHATOT (
    reshet_name VARCHAR(255),
    reshet_num BIGINT,
    vendor_website VARCHAR(100),
    user_name VARCHAR(100),
    password VARCHAR(255),
    file_date TIMESTAMP
);
