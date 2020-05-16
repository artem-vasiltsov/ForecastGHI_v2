import datetime
import psycopg2

from settings import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER, DB_PORT


class PostgresManage:

    def __init__(self):

        self.conn = self.__connect_db()
        self.cur = self.conn.cursor()
        self.__create_table()

    @staticmethod
    def __connect_db():

        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, database=DB_NAME, password=DB_PASSWORD)

        return conn

    def __create_table(self):

        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.average_x_value_khars(
                ID SERIAL PRIMARY KEY,                 
                TSTAMP timestamp NOT NULL,
                GHI_10_AVERAGE real NOT NULL,
                DHI_10_AVERAGE real NOT NULL,
                DNI_10_AVERAGE real NOT NULL 
            )
            """
        )

        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.y_value_khars(
                ID SERIAL PRIMARY KEY,
                TSTAMP timestamp NOT NULL,
                SOLAR_ANGLES text not null,
                y1 real NOT NULL,
                y1_corrected real not null,
                y2 real NOT NULL,
                y3 real NOT NULL,
                y4 real NOT NULL,
                y5 real NOT NULL,
                y6 real NOT NULL,
                y7 real NOT NULL,
                y8 real NOT NULL,
                y9 real NOT NULL,
                y10 real NOT NULL,
                y11 real NOT NULL,
                y12 real NOT NULL
            )
            """
        )

        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.forecast_visualization_khars(
                ID SERIAL PRIMARY KEY,                 
                TSTAMP timestamp NOT NULL,
                Forecast real NOT NULL
            )
            """
        )
        self.conn.commit()

    def read_average_x_value(self):

        avg_x_dict = {}

        select_query = "select * from average_x_value_khars"
        self.cur.execute(select_query)
        avg_x_records = self.cur.fetchall()

        for row in avg_x_records:

            avg_x_dict[row[1]] = row[2]

        return avg_x_dict

    def insert_average_x_value(self, ghi_x_val, dhi_x_val, dni_x_val, t_stamp):

        insert_query = "insert into average_x_value_khars (TSTAMP, GHI_10_AVERAGE, DHI_10_AVERAGE, DNI_10_AVERAGE) " \
                       "values (%s, %s, %s, %s)"
        record_insert = (t_stamp, ghi_x_val, dhi_x_val, dni_x_val)
        self.cur.execute(insert_query, record_insert)

        self.conn.commit()

        return

    def insert_y_value(self, y_dict, t_stamp, slr_angles):

        insert_query = "insert into y_value_khars (TSTAMP, SOLAR_ANGLES, y1, y1_corrected, y2, y3, y4, y5, y6, " \
                       "y7, y8, y9, y10, y11, y12) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                       "%s, %s)"
        record_insert = (t_stamp, slr_angles, y_dict[1], y_dict["corrected"], y_dict[2], y_dict[3], y_dict[4],
                         y_dict[5], y_dict[6], y_dict[7], y_dict[8], y_dict[9], y_dict[10], y_dict[11], y_dict[12])

        self.cur.execute(insert_query, record_insert)

        self.conn.commit()

        return

    def insert_forecast_visualization(self, t_stamp, y_value):

        del_query = "delete from forecast_visualization_khars"
        self.cur.execute(del_query)

        time_stamp = t_stamp
        for item in y_value:

            if item == "corrected":
                continue

            time_stamp += datetime.timedelta(hours=1, minutes=0, seconds=0)
            insert_query = "insert into forecast_visualization_khars (TSTAMP, Forecast) values (%s, %s)"
            record_insert = (time_stamp, y_value[item])

            self.cur.execute(insert_query, record_insert)

        self.conn.commit()

        return


if __name__ == '__main__':

    PostgresManage().read_average_x_value()
