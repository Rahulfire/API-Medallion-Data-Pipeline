Buckets Names
Bronze Bucket Name - api-medallion-bronze
Silver Bucket Name - api-medallion-silver
Gold Bucket Name - api-medallion-gold
Scripts Bucket Name- api-medallion-scripts

SNS Notifications
SNS ARN - arn:aws:sns:us-east-1:288606663251:api-medallion-alerts:ba08438e-d630-4e66-815f-dbc6ac1b9749

Glue Database Catalog
Glue Bronze Databases - api_medallion_bronze
Glue Silver Databases - api_medallion_silver
Glue Gold Databases - api_medallion_gold

Silver Publish
--bronze_database - api_medallion_bronze
--bronze_table - raw_statistics
--silver_bucket- api-medallion-silver
--silver_database - api_medallion_silver
--silver_table - clean_statistics

Gold Publish
--silver_database - api_medallion_silver
--gold_bucket - api-medallion-gold
--gold_database - api_medallion_gold
