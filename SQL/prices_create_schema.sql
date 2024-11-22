CREATE SCHEMA RAW_DATA;

CREATE TABLE RAW_DATA.SNIFIM (
    STOREID INT,
    BIKORETNO INT,
    STORETYPE INT,
    STORENAME VARCHAR(100),
    ADDRESS VARCHAR(255),
    CITY VARCHAR(100),
    ZIPCODE VARCHAR(20),
	file_name varchar(255) NULL,
	num_reshet bigint NULL,
	num_snif int NULL,
	file_date TIMESTAMP NULL
);


CREATE TABLE RAW_DATA.PRICES(
	PriceUpdateDate TIMESTAMP NULL,
	ItemCode bigint NULL,
	ItemType int NULL,
	ItemName varchar(255) NULL,
	ManufacturerName varchar(100) NULL,
	ManufactureCountry varchar(100) NULL,
	ManufacturerItemDescription varchar(255) NULL,
	UnitQty varchar(50) NULL,
	Quantity float NULL,
	UnitOfMeasure varchar(50) NULL,
	bIsWeighted int NULL,
	QtyInPackage varchar(100) NULL,
	ItemPrice float NULL,
	UnitOfMeasurePrice float NULL,
	AllowDiscount int NULL,
	ItemStatus int NULL,
	ItemId int NULL,
	file_name varchar(255) NULL,
	num_reshet bigint NULL,
	num_snif int NULL,
	file_date TIMESTAMP NULL
);


CREATE TABLE RAW_DATA.RESHATOT (
    reshet_name varchar(255),
    reshet_num bigint,
    vendor_website varchar(100),
    user_name varchar(100),
    password varchar(255),
	file_date TIMESTAMP
);