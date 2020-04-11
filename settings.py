import datetime
import os

now = datetime.datetime.now()

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DAT_FILE_PATH = ""
CONSTANTS_FILE = os.path.join(ROOT_DIR, 'utils', 'constants', 'GHI forecastng coefficients.xlsx')
SOLAR_ANGLE_FILE = os.path.join(ROOT_DIR, 'utils', 'constants', 'solarAngle.csv')

COUNTER = 12
DELAY_TIME = 60
START_TIME = datetime.datetime(2020, 1, 1, 1, 0, 0)
BASE_SOLAR_ANGLES = 80

DB_HOST = "localhost"
DB_NAME = "postgres"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = "123"
MEASUREMENT_TABLE_NAME = "measurements_v1"
