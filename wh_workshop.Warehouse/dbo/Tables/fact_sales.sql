CREATE TABLE [dbo].[fact_sales] (

	[order_id] varchar(50) NULL, 
	[order_item_id] int NULL, 
	[order_date_key] date NULL, 
	[customer_key] varchar(50) NULL, 
	[seller_key] varchar(50) NULL, 
	[product_key] varchar(50) NULL, 
	[revenue] decimal(12,2) NULL, 
	[freight] decimal(12,2) NULL, 
	[gross_value] decimal(12,2) NULL, 
	[is_late_delivery] bit NULL, 
	[is_currently_overdue] bit NULL, 
	[shipping_limit_date] datetime2(6) NULL, 
	[order_delivered_carrier_date] datetime2(6) NULL, 
	[order_status] varchar(20) NULL, 
	[order_estimated_delivery_date] datetime2(6) NULL, 
	[order_delivered_customer_date] datetime2(6) NULL, 
	[is_revenue_recognised] bit NULL
);