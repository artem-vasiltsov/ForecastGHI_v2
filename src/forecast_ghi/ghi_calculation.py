import datetime

from settings import COUNTER, START_TIME, BASE_SOLAR_ANGLES
from src.db_process.postgres_management import PostgresManage
from src.ghi_value_collection.ghi_value_result import extract_time_ghi_value
from utils.constants import load_constants_y, load_solar_angle


class ForecastGHI:

    def __init__(self):

        self.db_manage = PostgresManage()
        self.constants = load_constants_y()
        self.solar_angles_time, self.solar_angles = load_solar_angle()

    def forecast_y_value_ghi(self):

        last_record_time = self.db_manage.read_average_x_value()

        if last_record_time == {}:
            current_time = START_TIME
        else:
            current_time = list(last_record_time.keys())[-1] + \
                           datetime.timedelta(hours=1, minutes=0, seconds=0)

        while True:

            st_ghi_value, st_dhi_value, st_dni_value, str_dt_time = extract_time_ghi_value(dt_time=current_time)
            self.db_manage.insert_average_x_value(ghi_x_val=st_ghi_value, dhi_x_val=st_dhi_value,
                                                  dni_x_val=st_dni_value, t_stamp=str_dt_time)
            current_time += datetime.timedelta(hours=1, minutes=0, seconds=0)
            self.get_y_value()

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

            for i in range(1, 13):

                y_value[i] = self.constants[0][i]

                for j, avg_x_key in enumerate(list(avg_x_value)[-COUNTER:]):
                    y_value[i] += self.constants[COUNTER - j][i] * float(avg_x_value[avg_x_key])

            if float(solar_angles) > BASE_SOLAR_ANGLES:
                y_value["corrected"] = 0
            else:
                y_value["corrected"] = y_value[1]

            print("current time:{}".format(end_t_stamp))
            print("Y values:{}".format(y_value))

            self.db_manage.insert_y_value(y_dict=y_value, t_stamp=end_t_stamp, slr_angles=solar_angles)
            self.db_manage.insert_forecast_visualization(t_stamp=end_t_stamp, y_value=y_value)

            return


if __name__ == '__main__':

    ForecastGHI().forecast_y_value_ghi()
