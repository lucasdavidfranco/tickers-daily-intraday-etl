
import sys
import os 
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def etl_intradiary_analytics():
        
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    
    insert_intradiary_data = f"""

        INSERT INTO "{redshift_schema}".analytics_fact_daily_detail_tickers (
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
            minute_volume_amount_variation, 
            previous_close_value,
            minute_close_value_variation, 
            audit_datetime
        )
        with last_updated_at as (
            select 
                s.ticker,
                max(s.event_datetime) as last_event_datetime,
                max(date_add('minute', -5, s.event_datetime)) as last_event_datetime_window
            from "{redshift_schema}".analytics_fact_daily_detail_tickers as s
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
                case 
                    when s.event_datetime > coalesce(d.last_event_datetime, cast('2000-01-01' as timestamp)) then 1
                    else 0
                end as update_flag
            from "{redshift_schema}".staging_intraday_tickers as s
            left join last_updated_at as d on s.ticker = d.ticker
            where s.event_datetime >= coalesce(d.last_event_datetime_window, cast('2000-01-01' as timestamp))
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
        print("Table analytics_fact_daily_detail_tickers up to date")
        connection.close

    except Exception as e:
                
        print(f"Could not update analytics_fact_daily_detail_tickers: {e}\n")
        connection.close()
    

def etl_daily_analytics():
        
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
       
    insert_daily_data = f"""

        INSERT INTO "{redshift_schema}".analytics_fact_daily_summary_tickers (
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
            daily_volume_amount_variation,
            previous_close_value,
            daily_close_value_variation, 
            audit_datetime
        )
        with last_updated_at as (
            select 
                s.ticker,
                max(s.event_date) as last_event_date,
                max(date_add('day', -5, s.event_date)) as last_event_date_window
            from "{redshift_schema}".analytics_fact_daily_summary_tickers as s
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
                case 
                    when s.event_date > coalesce(d.last_event_date, cast('2000-01-01' as date)) then 1
                    else 0
                end as update_flag
            from "{redshift_schema}".staging_daily_tickers as s
            left join last_updated_at as d on s.ticker = d.ticker
            where s.event_date >= coalesce(d.last_event_date_window, cast('2000-01-01' as date))
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
        print("Table analytics_fact_daily_summary_tickers up to date.")
        connection.close()

    except Exception as e:
                    
        print(f"Could not update analytics_fact_daily_summary_tickers: {e}\n")
        connection.close()
