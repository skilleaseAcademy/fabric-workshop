CREATE TABLE [dbo].[dim_date] (

	[date_key] date NULL, 
	[year] int NULL, 
	[quarter] int NULL, 
	[month] int NULL, 
	[month_name] varchar(20) NULL, 
	[day_of_month] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(20) NULL, 
	[week_of_year] int NULL, 
	[year_month] varchar(7) NULL, 
	[is_weekend] bit NULL
);