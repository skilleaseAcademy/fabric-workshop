CREATE TABLE [dbo].[dim_customer] (

	[customer_key] varchar(50) NULL, 
	[customer_unique_id] varchar(50) NULL, 
	[customer_city] varchar(100) NULL, 
	[customer_state] varchar(5) NULL, 
	[customer_zip_code_prefix] varchar(10) NULL
);