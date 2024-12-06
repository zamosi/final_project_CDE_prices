CREATE TABLE hive.raw_data.PRICES (
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
) WITH(
		external_location = 's3a://prices/',
		format = 'PARQUET'
);