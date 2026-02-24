/*
===============================================================================
Procedure : bronze.proc_load_bronze
Purpose   : Load bronze layer tables from CRM and ERP CSV source files.
Stages    : CRM customer, CRM product, CRM sales, ERP location,
            ERP customer, ERP product category.
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.proc_load_bronze AS
BEGIN 
    DECLARE @startTime DATETIME, @endTime DATETIME;
    BEGIN TRY
        PRINT 'Starting bronze load process...';
        SET @startTime = GETDATE();
        PRINT 'Stage 1/6: Loading CRM customer info (bronze.crm_cust_info)...';
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'F:\- Programowanie -\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Stage 1/6 completed in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        SET @startTime = GETDATE();
        PRINT 'Stage 2/6: Loading CRM product info (bronze.crm_prd_info)...';
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'F:\- Programowanie -\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Stage 2/6 completed in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        SET @startTime = GETDATE();
        PRINT 'Stage 3/6: Loading CRM sales details (bronze.crm_sales_details)...';
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'F:\- Programowanie -\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Stage 3/6 completed in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        SET @startTime = GETDATE();
        PRINT 'Stage 4/6: Loading ERP location data (bronze.erp_loc_a101)...';
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'F:\- Programowanie -\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Stage 4/6 completed in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        SET @startTime = GETDATE(); 
        PRINT 'Stage 5/6: Loading ERP customer data (bronze.erp_cust_az12)...';
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'F:\- Programowanie -\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Stage 5/6 completed in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        SET @startTime = GETDATE(); 
        PRINT 'Stage 6/6: Loading ERP product category data (bronze.erp_px_cat_g1v2)...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'F:\- Programowanie -\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Stage 6/6 completed in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        PRINT 'Bronze load process completed successfully.';
        END TRY

    BEGIN CATCH
        PRINT 'An error occurred during the bronze load process.';
        PRINT ERROR_MESSAGE();
        PRINT ERROR_NUMBER();
    END CATCH;
END
