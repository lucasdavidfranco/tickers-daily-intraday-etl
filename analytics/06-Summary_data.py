
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import requests
import pandas as pd
import sys
import utils.db_utils as db_utils

# DEFINICION DE LA CONEXION A REDSHIFT #

redshift_schema = db_utils.import_db_variables()['redshift_schema']
connection = db_utils.connect_to_redshift()
tickers = db_utils.import_api_variables()['tickers']

for ticker in tickers:
    
    is_incremental = f"""select max(event_date) as q from "{redshift_schema}".analytics_fact_daily_summary_tickers where ticker = '{ticker}'"""

    staging_updated = f"""select max(event_date) as q from "{redshift_schema}".staging_daily_tickers where ticker = '{ticker}'"""

    max_analytics_date = connection.execute(is_incremental).fetchone()
    
    max_staging_date = connection.execute(staging_updated).fetchone()

    if max_analytics_date[0] is not None and max_staging_date[0] > max_analytics_date[0]:
        
        max_analytics_date_value = max_analytics_date[0]
        
        insert_daily_data = f"""

            INSERT INTO "{redshift_schema}".analytics_fact_daily_summary_tickers (event_date, ticker, open_value,
                high_value, low_value, close_value, volume_amount, close_value_sma, volume_sma,
                previous_volume_amount, daily_volume_amount_variation, previous_close_value,
                daily_close_value_variation, audit_datetime
            )
            with date_from as (
                select 
                    ticker,
                    dateadd('day', -5, min(event_date)) as event_date_window,
                    min(event_date) as event_date_update
                from "{redshift_schema}".staging_daily_tickers as s
                where 1 = 1 
                    and event_date > cast('{max_analytics_date_value}' as timestamp)
                    and ticker = '{ticker}'
                group by 1
            ),
            base as (
                select 
                    s.event_date,
                    s.ticker,
                    s.open_value,
                    s.high_value,
                    s.low_value,
                    s.close_value,
                    s.volume_amount,
                    avg(s.close_value) over (partition by s.ticker order by s.event_date rows between 4 preceding and current row) as close_value_sma,
                    avg(s.volume_amount) over (partition by s.ticker order by s.event_date rows between 4 preceding and current row) as volume_sma,
                    lag(s.volume_amount) over (partition by s.ticker order by s.event_date) as previous_volume_amount,
                    lag(s.close_value) over (partition by s.ticker order by s.event_date) as previous_close_value,
                    if(s.event_date >= d.event_date_update, 1, 0) as update_flag
                from "{redshift_schema}".staging_daily_tickers as s
                inner join date_from as d on s.ticker = d.ticker
                    and s.event_date >= d.event_date_window
                    and s.ticker = '{ticker}'
                where 1 = 1
            )
            select 
                event_date,
                ticker,
                open_value,
                high_value,
                low_value,
                close_value,
                volume_amount,
                close_value_sma,
                volume_sma,
                previous_volume_amount,
                100 * ( volume_amount / previous_volume_amount - 1) as daily_volume_amount_variation,
                previous_close_value,
                100 * ( close_value / previous_close_value - 1) as daily_close_value_variation,
                current_timestamp as audit_datetime
            from base
            where 1 = 1
                and update_flag = 1;
        """

        try:
                
            connection.execute(insert_daily_data)

            print(f"{ticker} daily incremental data uploaded to analytics fact summary table\n")    

        except Exception as e:
                
            print(f"{ticker} could not upload daily incremental data to analytics fact summary table: {e}\n")
            
            sys.exit("End of process")
        
    
    elif max_analytics_date[0] is not None and max_staging_date[0] == max_analytics_date[0]:
        
        print(f"{ticker} daily incremental has no updates to upload to analytics fact summary table\n")    
        
    else:
        
        insert_historic_data = f"""

            INSERT INTO "{redshift_schema}".analytics_fact_daily_summary_tickers (event_date, ticker, open_value,
                high_value, low_value, close_value, volume_amount, close_value_sma, volume_sma,
                previous_volume_amount, daily_volume_amount_variation, previous_close_value,
                daily_close_value_variation, audit_datetime
            )
            with base as (
                select 
                    s.event_date,
                    s.ticker,
                    s.open_value,
                    s.high_value,
                    s.low_value,
                    s.close_value,
                    s.volume_amount,
                    avg(s.close_value) over (partition by s.ticker order by s.event_date rows between 4 preceding and current row) as close_value_sma,
                    avg(s.volume_amount) over (partition by s.ticker order by s.event_date rows between 4 preceding and current row) as volume_sma,
                    lag(s.volume_amount) over (partition by s.ticker order by s.event_date) as previous_volume_amount,
                    lag(s.close_value) over (partition by s.ticker order by s.event_date) as previous_close_value
                from "{redshift_schema}".staging_daily_tickers as s
                where 1 = 1
                    and s.ticker = '{ticker}'
            )
            select 
                event_date,
                ticker,
                open_value,
                high_value,
                low_value,
                close_value,
                volume_amount,
                close_value_sma,
                volume_sma,
                previous_volume_amount,
                100 * ( volume_amount / previous_volume_amount - 1) as daily_volume_amount_variation,
                previous_close_value,
                100 * ( close_value / previous_close_value - 1) as daily_close_value_variation,
                current_timestamp as audit_datetime
            from base
            where 1 = 1;

        """
    
        try:
                
            connection.execute(insert_historic_data)

            print(f"{ticker} historic data uploaded to analytics fact summary table\n")

        except Exception as e:
                
            print(f"{ticker} could not upload daily data to analytics fact summary table: {e}\n")
            
            sys.exit("End of process")
