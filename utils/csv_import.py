import psycopg2
import os

from settings import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER, ROOT_DIR, DB_PORT


def insert_csv_tmp_data():

    test_csv_file = os.path.join(ROOT_DIR, 'test_data', 'sample.csv')
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, database=DB_NAME, password=DB_PASSWORD)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS measurements_v1(
            logger_id text, 
            station_name text COLLATE pg_catalog."default",
            tstamp timestamp with time zone NOT NULL,
            pyrgeometer_body_temperature_avg text,
            pyrgeometer_body_temperature_max text,
            pyrgeometer_body_temperature_min text,
            pyrgeometer_body_temperature_stdev text,
            pyrgeometer_body_temperature_count text,
            dhi_solar_irradiance_avg text,
            dhi_solar_irradiance_max text,
            dhi_solar_irradiance_min text,
            dhi_solar_irradiance_stdev text,
            dhi_solar_irradiance_count text,
            ghi_solar_irradiance_avg text,
            ghi_solar_irradiance_max text,
            ghi_solar_irradiance_min text,
            ghi_solar_irradiance_stdev text,
            ghi_solar_irradiance_count text,
            eppley_tuvr_voltage_avg text,
            eppley_tuvr_voltage_max text,
            eppley_tuvr_voltage_min text,
            eppley_tuvr_voltage_stdev text,
            eppley_tuvr_voltage_count text,
            dni_voltage_avg text,
            dni_voltage_max text,
            dni_voltage_min text,
            dni_voltage_stdev text,
            dni_voltage_count text,
            pyrgeometer_voltage_avg text,
            pyrgeometer_voltage_max text,
            pyrgeometer_voltage_min text,
            pyrgeometer_voltage_stdev text,
            pyrgeometer_voltage_count text,
            pyrgeometer_ein_solar_ir_avg text,
            pyrgeometer_ein_solar_ir_max text,
            pyrgeometer_ein_solar_ir_min text,
            pyrgeometer_ein_solar_ir_stdev text,
            pyrgeometer_ein_solar_ir_count text,
            pyrgeometer_enet_solar_ir_avg text,
            pyrgeometer_enet_solar_ir_max text,
            pyrgeometer_enet_solar_ir_min text,
            pyrgeometer_enet_solar_ir_stdev text,
            pyrgeometer_enet_solar_ir_count text,
            a1_avg text,
            a1_max text,
            a1_min text,
            a1_stdev text,
            a1_count text,
            a2_avg text,
            a2_max text,
            a2_min text,
            a2_stdev text,
            a2_count text,
            a3_avg text,
            a3_max text,
            a3_min text,
            a3_stdev text,
            a3_count text,
            a4_avg text,
            a4_max text,
            a4_min text,
            a4_stdev text,
            a4_count text,
            a5_avg text,
            a5_max text,
            a5_min text,
            a5_stdev text,
            a5_count text,
            a6_avg text,
            a6_max text,
            a6_min text,
            a6_stdev text,
            a6_count text,
            voltage_avg text,
            voltage_max text,
            voltage_min text,
            current_avg text,
            current_max text,
            current_min text,
            temp_avg text,
            addr bigint        
        )
        """
    )

    with open(test_csv_file, 'r') as f:
        next(f)
        cur.copy_from(f, 'measurements_v1', sep=',')

    conn.commit()


if __name__ == '__main__':

    insert_csv_tmp_data()
