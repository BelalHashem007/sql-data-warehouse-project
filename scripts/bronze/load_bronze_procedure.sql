/*
===============================================================
Stored Procedure Script: Creates a new Stored Procedure
===============================================================
Script Purpose:
    This script creates a new stored procedure on bronze schema
    called bronze.load_bronze and it's used to load data from
    csv files into bronze layer tables.
    It first truncates the tables then load the data from csv
    files.

    Usage Example :
        EXEC bronze.load_bronze;
===============================================================
*/
-- Stored Procedure to load data into bronze layer
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_total_time DATETIME, @end_total_time DATETIME;
-- Bulk insert data into tables
    BEGIN TRY

        PRINT '=========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=========================================';
        -- crm tables
        PRINT '-----------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------------------';

        SET @start_total_time= GETDATE();
        SET @start_time = GETDATE();

        PRINT '>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/home/belalbright/SQL/DW/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time To Load: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';


        SET @start_time = GETDATE();

        PRINT '>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/home/belalbright/SQL/DW/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time To Load: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/home/belalbright/SQL/DW/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time To Load: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        -- erp tables
        PRINT '-----------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------------------------';

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/home/belalbright/SQL/DW/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time To Load: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/home/belalbright/SQL/DW/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time To Load: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';


        SET @start_time = GETDATE();

        PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/home/belalbright/SQL/DW/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Time To Load: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT '==========================================';
    END CATCH
    SET @end_total_time = GETDATE();
    PRINT '==========================================';
    PRINT 'Time To Load All Tables: ' + CAST (DATEDIFF(SECOND,@start_total_time,@end_total_time) AS NVARCHAR) + ' seconds';
END;
