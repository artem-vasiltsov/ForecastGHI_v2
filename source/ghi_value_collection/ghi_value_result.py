import time

from settings import STATIONS, DELAY_TIME
from source.db_process.postgres_management import PostgresManage
from utilis.data_separation import estimate_data_time
from utilis.date_time import convert_datetime
from utilis.time_delay import estimate_data_accuracy


class GhiCollector:

    def __init__(self):

        self.db_manage = PostgresManage()

    def extract_station_ghi_value(self, dt_time):

        station_value = {}
        for station in STATIONS:
            station_value[station] = self.db_manage.read_ghi_value(date_time=dt_time, station_name=station)

        return station_value

    def extract_time_ghi_value(self, dt_time):

        time_value = self.extract_station_ghi_value(dt_time=dt_time)
        str_dt_time = convert_datetime(dt_time)
        past_present_value = estimate_data_time(data_time=dt_time)

        if past_present_value == "present":

            data_accuracy = estimate_data_accuracy(data=time_value)

            cur_index = 0
            while not data_accuracy:

                cur_index += 1
                time.sleep(60)
                time_value = self.extract_station_ghi_value(dt_time=dt_time)
                data_accuracy = estimate_data_accuracy(data=time_value)
                if cur_index > DELAY_TIME:
                    break

        return time_value, str_dt_time
