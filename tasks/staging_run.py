
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
from staging import (create_staging_tables , etl_staging_intradiary, etl_staging_daily)

def staging_run():
    
    create_staging_tables.create_staging_tables()
    
    etl_staging_intradiary.load_intradiary_data()
    
    etl_staging_daily.load_daily_data()
    
if __name__ == "__main__":
    staging_run()
