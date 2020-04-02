def convert_datetime(date_time):

    dt_time = date_time.strftime("%Y-%m-%d %H:%M:00")
    dt_time += "+03"

    return dt_time
