CREATE OR REPLACE STORAGE INTEGRATION S3_RT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::748317758880:user/snowflake_real_time_project'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-db-tutorial-om/stream_data/')
COMMENT = 'CONNECTION TO S3 FOR Real Time Pipeline';


CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage
URL='s3://snowflake-db-tutorial-om/stream_data/'
STORAGE_INTEGRATION = S3_RT
   

CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

SHOW STAGES;
LIST @customer_ext_stage;


CREATE OR REPLACE PIPE customer_s3_pipe
  auto_ingest = true
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = CSV
  ;

select * from customer_raw;

show pipes;
select SYSTEM$PIPE_STATUS('customer_s3_pipe');

SELECT count(*) FROM customer_raw limit 10;

TRUNCATE  customer_raw;