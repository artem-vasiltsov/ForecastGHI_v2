import datetime

from settings import DELAY_TIME


def estimate_data_time(data_time):

    current_time = datetime.datetime.now()
    diff_current_data_time = current_time - data_time
    diff_min = int(diff_current_data_time.total_seconds() / 60)

    if diff_min > DELAY_TIME:

        data_type = "past"
    else:

        data_type = "present"

    return data_type


if __name__ == '__main__':

    estimate_data_time(data_time="")
