COPY (
    select invoice_number,
           stock_code,
           detail,
           quantity,
           invoice_date,
           unit_price,
           customer_id,
           country
      from retail.user_purchase
     where quantity > 2
       and cast(invoice_date as date)='{{ ds }}') --this is a Airflow macro to get the date
TO '{{ params.temp_filtered_user_purchase }}' WITH (FORMAT CSV, HEADER);
--the location here is for the DAG