import pandas as pd

from settings import CONSTANTS_FILE, COUNTER, SOLAR_ANGLE_FILE


def load_constants_y():

    constants = {}
    constants_df = pd.read_excel(CONSTANTS_FILE, "Sheet1")

    for i in range(COUNTER + 1):

        constants[i] = {}
        for j in range(1, 13):

            constants[i][j] = constants_df["a" + str(i)][j - 1]

    return constants


def load_solar_angle():

    solar_angle_data = pd.read_csv(SOLAR_ANGLE_FILE, header=None)
    solar_angles_time = solar_angle_data.iloc[:, 0].values.tolist()[1:]
    solar_angles = solar_angle_data.iloc[:, 1].values.tolist()[1:]

    return solar_angles_time, solar_angles


if __name__ == '__main__':

    # load_constants_y()
    load_solar_angle()
