
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import sys
import utils.db_utils as db_utils

# DEFINICION DE LA CONEXION A REDSHIFT #

redshift_schema = db_utils.import_db_variables()['redshift_schema']
connection = db_utils.connect_to_redshift()
tickers = db_utils.import_api_variables()['tickers']
    
# CREACION DE FACT TABLE A PARTIR DE STAGING INTRADIARIO # 
    
for ticker in tickers:
    
    is_incremental = f"""select max(event_datetime) as q from "{redshift_schema}".analytics_fact_daily_detail_tickers where ticker = '{ticker}'"""

    staging_updated = f"""select max(event_datetime) as q from "{redshift_schema}".staging_intraday_tickers where ticker = '{ticker}'"""

    max_analytics_datetime = connection.execute(is_incremental).fetchone()
    
    max_staging_datetime = connection.execute(staging_updated).fetchone()

    if max_analytics_datetime[0] is not None and max_staging_datetime[0] > max_analytics_datetime[0]:
        
        max_analytics_datetime_value = max_analytics_datetime[0]
        
        insert_intradiary_data = f"""

            INSERT INTO "{redshift_schema}".analytics_fact_daily_detail_tickers (event_datetime, ticker, open_value,
                high_value, low_value, close_value, volume_amount, close_value_sma, volume_sma,
                previous_volume_amount, minute_volume_amount_variation, previous_close_value,
                minute_close_value_variation, audit_datetime
            )
            with datetime_from as (
                select 
                    ticker,
                    dateadd('minute', -5, min(event_datetime)) as event_datetime_window,
                    min(event_datetime) as event_datetime_update
                from "{redshift_schema}".staging_intraday_tickers as s
                where 1 = 1 
                    and event_datetime > cast('{max_analytics_datetime_value}' as timestamp)
                    and ticker = '{ticker}'
                group by 1
            ),
            base as (
                select 
                    s.event_datetime,
                    s.ticker,
                    s.open_value,
                    s.high_value,
                    s.low_value,
                    s.close_value,
                    s.volume_amount,
                    avg(s.close_value) over (partition by s.ticker order by s.event_datetime rows between 4 preceding and current row) as close_value_sma,
                    avg(s.volume_amount) over (partition by s.ticker order by s.event_datetime rows between 4 preceding and current row) as volume_sma,
                    lag(s.volume_amount) over (partition by s.ticker order by s.event_datetime) as previous_volume_amount,
                    lag(s.close_value) over (partition by s.ticker order by s.event_datetime) as previous_close_value,
                    if(s.event_datetime >= d.event_datetime_update, 1, 0) as update_flag
                from "{redshift_schema}".staging_intraday_tickers as s
                inner join datetime_from as d on s.ticker = d.ticker
                    and s.event_datetime >= d.event_datetime_window
                    and s.ticker = '{ticker}'
                where 1 = 1
            )
            select 
                event_datetime,
                ticker,
                open_value,
                high_value,
                low_value,
                close_value,
                volume_amount,
                close_value_sma,
                volume_sma,
                previous_volume_amount,
                100 * ( volume_amount / previous_volume_amount - 1) as minute_volume_amount_variation,
                previous_close_value,
                100 * ( close_value / previous_close_value - 1) as minute_close_value_variation,
                current_timestamp as audit_datetime
            from base
            where 1 = 1
                and update_flag = 1;
        """

        try:
                
            connection.execute(insert_intradiary_data)

            print(f"{ticker} intraday incremental data uploaded to analytics fact detail table\n")    

        except Exception as e:
                
            print(f"{ticker} could not upload intraday incremental data to analytics fact detail table: {e}\n")
            
            sys.exit("End of process")
        
    
    elif max_analytics_datetime[0] is not None and max_staging_datetime[0] == max_analytics_datetime[0]:
        
        print(f"{ticker} intraday incremental has no updates to upload to analytics fact detail table\n")    
        
    else:
        
        insert_historic_data = f"""

            INSERT INTO "{redshift_schema}".analytics_fact_daily_detail_tickers (event_datetime, ticker, open_value,
                high_value, low_value, close_value, volume_amount, close_value_sma, volume_sma,
                previous_volume_amount, minute_volume_amount_variation, previous_close_value,
                minute_close_value_variation, audit_datetime
            )
            with base as (
                select 
                    s.event_datetime,
                    s.ticker,
                    s.open_value,
                    s.high_value,
                    s.low_value,
                    s.close_value,
                    s.volume_amount,
                    avg(s.close_value) over (partition by s.ticker order by s.event_datetime rows between 4 preceding and current row) as close_value_sma,
                    avg(s.volume_amount) over (partition by s.ticker order by s.event_datetime rows between 4 preceding and current row) as volume_sma,
                    lag(s.volume_amount) over (partition by s.ticker order by s.event_datetime) as previous_volume_amount,
                    lag(s.close_value) over (partition by s.ticker order by s.event_datetime) as previous_close_value
                from "{redshift_schema}".staging_intraday_tickers as s
                where 1 = 1
                    and s.ticker = '{ticker}'
            )
            select 
                event_datetime,
                ticker,
                open_value,
                high_value,
                low_value,
                close_value,
                volume_amount,
                close_value_sma,
                volume_sma,
                previous_volume_amount,
                100 * ( volume_amount / previous_volume_amount - 1) as minute_volume_amount_variation,
                previous_close_value,
                100 * ( close_value / previous_close_value - 1) as minute_close_value_variation,
                current_timestamp as audit_datetime
            from base
            where 1 = 1;

        """
    
        try:
                
            connection.execute(insert_historic_data)

            print(f"{ticker} historic data uploaded to analytics fact detail table\n")

        except Exception as e:
                
            print(f"{ticker} could not upload intraday data to analytics fact detail table: {e}\n")
            
            sys.exit("End of process")
