import datetime

from settings import TIME_INTERVAL, AVG_COUNT, COUNTER, START_TIME, STATIONS, BASE_SOLAR_ANGLES
from source.db_process.postgres_management import PostgresManage
from source.ghi_value_collection.ghi_value_result import GhiCollector
from utilis.constants import load_constants_y, load_solar_angle


class ForecastGHI:

    def __init__(self):

        self.db_manage = PostgresManage()
        self.constants = load_constants_y()
        self.solar_angles_time, self.solar_angles = load_solar_angle()
        self.ghi_collector = GhiCollector()

    def forecast_y_value_ghi(self):

        x_value = {}

        last_record_time = self.db_manage.read_average_x_value()

        if last_record_time == {}:
            current_time = START_TIME
        else:
            current_time = list(last_record_time.keys())[-1] + \
                           datetime.timedelta(hours=0, minutes=TIME_INTERVAL, seconds=0)

        st_ghi_value, str_dt_time = self.ghi_collector.extract_time_ghi_value(dt_time=current_time)
        x_value[str_dt_time] = st_ghi_value

        next_time = current_time + datetime.timedelta(hours=0, minutes=TIME_INTERVAL, seconds=0)

        while True:
            temp_x, str_next_time = self.ghi_collector.extract_time_ghi_value(dt_time=next_time)

            x_value[str_next_time] = temp_x
            print(x_value)
            x_value = self.get_average_x_value(x_dict=x_value)
            next_time += datetime.timedelta(hours=0, minutes=TIME_INTERVAL, seconds=0)
            # print(x_value)

    def get_average_x_value(self, x_dict):

        avg_x = {}
        sum_x = {}
        avg_cnt = {}

        if len(x_dict) < AVG_COUNT:

            return x_dict
        else:

            for station in STATIONS:
                sum_x[station] = 0
                avg_cnt[station] = AVG_COUNT

            for x in x_dict:

                for station_term in x_dict[x]:
                    sum_x[station_term] += float(x_dict[x][station_term])

                    if float(x_dict[x][station_term]) == 0:
                        avg_cnt[station_term] -= 1

            for y in sum_x:

                if avg_cnt[y] == 0:
                    avg_x[y] = 0
                    continue

                avg_x[y] = sum_x[y] / avg_cnt[y]

                print(y, avg_x[y])

            self.db_manage.insert_average_x_value(x_val=avg_x, t_stamp=list(x_dict.keys())[-1])
            self.get_y_value()

            x_dict.clear()

            return x_dict

    def get_y_value(self):

        avg_x_value = self.db_manage.read_average_x_value()

        if len(avg_x_value) < COUNTER:

            return

        else:

            y_value = {}

            end_t_stamp = list(avg_x_value.keys())[-1]
            end_minute = int(end_t_stamp.strftime("%M"))
            if end_minute >= 30:
                solar_angle_c_time = end_t_stamp + datetime.timedelta(hours=0, minutes=60 - end_minute, seconds=0)
            else:
                solar_angle_c_time = end_t_stamp - datetime.timedelta(hours=0, minutes=end_minute, seconds=0)

            solar_angles = self.solar_angles[self.solar_angles_time.index(
                solar_angle_c_time.strftime("%Y-%m-%d %H:%M:00"))]
            for station_term in STATIONS:

                for i in range(1, 13):

                    y_value[i] = self.constants[0][i]

                    for j, avg_x_key in enumerate(list(avg_x_value)[-COUNTER:]):
                        y_value[i] += self.constants[COUNTER - j][i] * \
                                      float(avg_x_value[avg_x_key][station_term])

                if float(solar_angles) > BASE_SOLAR_ANGLES:
                    y_value["corrected"] = 0
                else:
                    y_value["corrected"] = y_value[1]

                print("station:{}".format(station_term))
                print("current time:{}".format(end_t_stamp))
                print("Y values:{}".format(y_value))

                self.db_manage.insert_y_value(y_dict=y_value, t_stamp=end_t_stamp, station=station_term,
                                              slr_angles=solar_angles)

                self.db_manage.insert_forecast_visualization(t_stamp=end_t_stamp, y_value=y_value, station=station_term)

            return
