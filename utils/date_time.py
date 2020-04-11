import datetime


def convert_datetime(date_time):

    dt_time = date_time.strftime("%Y-%m-%d %H:%M:00")
    dt_time += "+03"
    dt = date_time.strftime("%Y-%m-%d")
    tm = date_time.strftime("%H:%M:%S")

    return dt_time, dt, tm


if __name__ == '__main__':

    convert_datetime(date_time=datetime.datetime.now())
