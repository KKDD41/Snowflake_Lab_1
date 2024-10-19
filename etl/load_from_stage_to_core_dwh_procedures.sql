CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ';'
field_optionally_enclosed_by='"'
SKIP_HEADER = 1
DATE_FORMAT = 'DD.MM.YY';

-- Procedure for NATION table
CREATE OR REPLACE PROCEDURE load_data_nation()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.NATION
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_nation.csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into NATION table';
END;

-- Procedure for CUSTOMER table
CREATE OR REPLACE PROCEDURE load_data_customer()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.CUSTOMER
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_customer.csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into CUSTOMER table';
END;

-- Procedure for PART table
CREATE OR REPLACE PROCEDURE load_data_part()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.PART
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_part.csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into PART table';
END;

-- Procedure for PARTSUPP table
CREATE OR REPLACE PROCEDURE load_data_partsupp()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.PARTSUPP
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_partsupp.csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into PARTSUPP table';
END;

-- Procedure for REGION table
CREATE OR REPLACE PROCEDURE load_data_region()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.REGION
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_region.csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into REGION table';
END;

-- Procedure for SUPPLIER table
CREATE OR REPLACE PROCEDURE load_data_supplier()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.SUPPLIER
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_supplier.csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into SUPPLIER table';
END;

-- Procedure for LINEITEM table
CREATE OR REPLACE PROCEDURE load_data_lineitem()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.LINEITEM
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_lineitem_[0-9].csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into LINEITEM table';
END;

-- Procedure for ORDERS table
CREATE OR REPLACE PROCEDURE load_data_orders()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO EPAM_LAB.CORE_DWH.ORDERS
    FROM @EPAM_LAB.CORE_DWH.STAGE
    PATTERN = 'h_order_[0-9].csv'
    FILE_FORMAT = CSV_FORMAT;
    RETURN 'Data loaded into ORDERS table';
END;


CREATE OR REPLACE PROCEDURE reload_data_from_stage_to_core_dwh_procedure()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN
    CALL create_tables_in_core_dwh('');

    CALL load_data_nation();
    CALL load_data_customer();
    CALL load_data_part();
    CALL load_data_partsupp();
    CALL load_data_region();
    CALL load_data_supplier();
    CALL load_data_lineitem();
    CALL load_data_orders();
    RETURN 'Tables in CORE_DWH were populated with data from STAGE.';
END;

