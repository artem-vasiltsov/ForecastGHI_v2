# ForecastGHI_v2

## Overview

This project is the second version of ForecastGHI. This is to forecast y value by GHI values gained in real time 
for one station. That logs are saved in disk, not database as a dat file format.

## Structure

- src

    The source code to save the data in dat file into the postgresql database, calculate the GHI value in real time.

- utils

    * The constant files for GHI forecast coefficients and solar Angles
    * The source code to load the coefficients and solar angles, process time and date, delay.

- app

    The main execution file
    
- requirements

    All the dependencies for this project
    
- settings

    The several settings for this project

## Installation

- Environment
    Ubuntu 18.04, python 3.6 postgresql 12.0 
    
- To install dependencies, run following command in terminal

    ```
        pip3 install -r requirements.txt
    ```    

- Note: In Ubuntu 18.04, before installing dependencies, install libpq-dev for psycopg2

    ```
        sudo apt install libpq-dev python3-dev
    ```        
    
## Configuration

In settings.py, please configure various options including database settings.

- DAT_FILE_PATH is a absolute path to dat file. Please insert your dat file absolute path.

- COUNTER is number of terms necessary for calculating y value. It's value is 12 and can be changed manually, but must
be less than 76.

- Also START_TIME can be configured, that can be easily changed by year, month, day, hour, minute as you like.
But at this point, second, where is set as 0, can never be changed as long as tstamp field of measurement table doesn't 
change.

- DELAY_TIME shows the max time that it takes to get each GHI value from the server. It's default value is 60 min.

- BASE_SOLAR_ANGLES is the threshold value for y1_corrected. If the value of solar angle is greater than this threshold
value for each measurement time, the value of y1_corrected is equal to the value of y1, else 0. It's default value is 80.

## Execution

- In terminal, run the following command.

    ```
        python3 app.py
    ``` 

## Output

Output of this project is three tables in your postgresql database, whose names are average_x_value_khars, y_value_khars
and forecast_visualization_khars.

Table "average_x_value_khars" contains average value of x every 60 minutes. And Table "y_value_khars" contains 
12 y_values after 12 steps, Table "forecast_visualization_khars" contains forecast y values for visualization.

