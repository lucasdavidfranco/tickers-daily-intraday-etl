
import sys
import os 
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def etl_fact_analytics():
    
    ''' Updates fact tables 
    
    Once we have all data on staging tables, this process is run to update analytics table with business logic
    
    For the upload 2 SQL scripts are run: 
    
    analytics_fact_daily_detail_tickers: To begin with this ETL, we retrieve last data available on this table
    After this, data on staging table is filtered based on last analytics data to get only data that has not been processed and upload to analytics yet.
    Once this is done several calculations are made such as previous values for a specific metric or SMA values (slowly moving average)
    On script there is an auxiliary field update_flag. This is used due to the fact that some metrics needs to have 5 previous records to be calculated
    Those records are retrived with last_event_datetime_window and then with upload_flag we filter those auxiliary records only brought to have calculations ok.
    
    analytics_fact_daily_summary_tickers: To begin with this ETL, we retrieve last data available on this table 
    After this, data on staging table is filtered based on last analytics data to get only data that has not been processed and upload to analytics yet.
    Once this is done several calculations are made such as previous values for a specific metric or SMA values (slowly moving average)
    On script there is an auxiliary field update_flag. This is used due to the fact that some metrics needs to have 5 previous records to be calculated
    Those records are retrived with last_event_date_window and then with upload_flag we filter those auxiliary records only brought to have calculations ok.
    
    '''
    
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    
    etl_process = {
        "analytics_fact_daily_detail_tickers": f"""
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
                case 
                    when previous_volume_amount = 0 and volume_amount = 0 then 0
                    when previous_volume_amount = 0 and volume_amount != 0 then 100
                    else 100 * ( volume_amount / previous_volume_amount - 1) 
                end as minute_volume_amount_variation,
                previous_close_value,
                case 
                    when close_value = 0 and previous_close_value = 0 then 0
                    when close_value = 0 and previous_close_value != 0 then 100
                    else 100 * ( close_value / previous_close_value - 1) 
                end as minute_close_value_variation,
                current_timestamp as audit_datetime
            from base
            where 1 = 1
                and update_flag = 1;
        """,
        "analytics_fact_daily_summary_tickers": f"""
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
                case 
                    when previous_volume_amount = 0 and volume_amount = 0 then 0
                    when previous_volume_amount = 0 and volume_amount != 0 then 100
                    else 100 * ( volume_amount / previous_volume_amount - 1)
                end as daily_volume_amount_variation,
                previous_close_value,
                case 
                    when close_value = 0 and previous_close_value = 0 then 0
                    when close_value = 0 and previous_close_value != 0 then 100
                    else 100 * ( close_value / previous_close_value - 1) 
                end as daily_close_value_variation,
                current_timestamp as audit_datetime
            from base
            where 1 = 1
                and update_flag = 1;
        """
    }
    
    for table_name, etl_definition in etl_process.items():
        
        connection.execute(etl_definition)
        print(f"Table {table_name} up to date\n")
        
    connection.close()
