create or replace function buss_hours(start_date string, end_date string)
returns int
language python
runtime_version = '3.8'
packages = ('numpy')
handler = 'buss_hours'
as
$$
def buss_hours(start_date, end_date):

    'Uses numpy.busday_count to calculate working hours between two dates'

    import numpy as np
    import datetime as dt

    try:
        start_year = int(start_date[0:4])
        start_month = int(start_date[5:7])
        start_day = int(start_date[8:10])
    
        end_year = int(end_date[0:4])
        end_month = int(end_date[5:7])
        end_day = int(end_date[8:10])
    
        start = dt.date(start_year, start_month, start_day)
        end = dt.date(end_year, end_month, end_day)
    
        days = np.busday_count(start, end)
    
        # 8 working hours per day:
        working_hours = 8*days
    
        return working_hours

    except:
        return 0

$$;