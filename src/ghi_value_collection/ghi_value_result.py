import time
import datetime

from settings import DELAY_TIME, DAT_FILE_PATH
from utils.date_time import convert_datetime


def get_ghi_value_dat_file(c_date, c_time):

    ghi_value = None
    dni_value = None
    dhi_value = None

    dat_content = [i.strip().split() for i in open(DAT_FILE_PATH).readlines()]
    for dat in dat_content[4:]:
        dat_date = dat[0][1:]
        dat_list = dat[1].split(",")
        dat_time = dat_list[0][:-1]
        if dat_date == c_date and dat_time == c_time:
            ghi_value = dat_list[2]
            dhi_value = dat_list[6]
            dni_value = dat_list[10]

            break

    return ghi_value, dhi_value, dni_value


def extract_time_ghi_value(dt_time):

    str_dt_time, current_date, current_time = convert_datetime(date_time=dt_time)
    ghi_value, dhi_value, dni_value = get_ghi_value_dat_file(c_date=current_date, c_time=current_time)

    while ghi_value is None or dni_value is None:
        time.sleep(DELAY_TIME * 60)
        ghi_value, dhi_value, dni_value = get_ghi_value_dat_file(c_date=current_date, c_time=current_time)

    return ghi_value, dhi_value, dni_value, str_dt_time


if __name__ == '__main__':

    extract_time_ghi_value(dt_time=datetime.datetime(2020, 1, 2, 1, 0, 0))
