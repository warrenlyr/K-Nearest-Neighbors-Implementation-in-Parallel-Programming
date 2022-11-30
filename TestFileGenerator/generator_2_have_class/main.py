'''
---------------------------------------------------------------------------
Description:
Clean multiple datasets and merge into one
data file that we can use for our final project.
Use Python because it's the easiest and fastest way
to develop the tool.
---------------------------------------------------------------------------
Datasets source:
https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data
---------------------------------------------------------------------------
Author: Warren Liu
Date: 11/30/2022
---------------------------------------------------------------------------
'''

import os
import matplotlib.pyplot as plt

ORG_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'original_datasets'
)

MERGED_DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'merged_datasets'
)
if not os.path.exists(MERGED_DATA_PATH):
    os.mkdir(MERGED_DATA_PATH)


def merge_datasets() -> bool:

    # read datasets
    humidity = open(os.path.join(ORG_DATA_PATH, 'humidity.csv'), 'r', 1, 'utf8')
    pressure = open(os.path.join(ORG_DATA_PATH, 'pressure.csv'), 'r', 1, 'utf8')
    temperature = open(os.path.join(ORG_DATA_PATH, 'temperature.csv'), 'r', 1, 'utf8')
    weather = open(os.path.join(ORG_DATA_PATH, 'weather_description.csv'), 'r', 1, 'utf8')

    humidity_lines = humidity.readlines()
    pressure_lines = pressure.readlines()
    temperature_lines = temperature.readlines()
    weather_lines = weather.readlines()

    humidity.close()
    pressure.close()
    temperature.close()
    weather.close()

    # file to write merged data
    merged_data = open(os.path.join(MERGED_DATA_PATH, 'data.csv'), 'w+', 1, 'utf8')

    # validate datasets
    if len(humidity_lines) != len(pressure_lines) != len(temperature_lines) != len(weather_lines):
        print('Datasets length unequal, terminated.')
        return False

    line_cnt = 0
    for (h, p, t, w) in list(zip(humidity_lines, pressure_lines, temperature_lines, weather_lines)):
        if not line_cnt: 
            line_cnt += 1
            continue

        h = h.split(',')[1:]
        p = p.split(',')[1:]
        t = t.split(',')[1:]
        w = w.split(',')[1:]

        for (hh, pp, tt, ww) in zip(h, p, t, w):
            hh = hh.strip()
            pp = pp.strip()
            tt = tt.strip()
            ww = ww.strip()
            
            # do some filter on the weather
            if 'rain' in ww.lower():
                ww = 'rain'
            elif 'snow' in ww.lower():
                ww = 'snow'
            elif 'clear' in ww.lower():
                ww = 'clear'
            elif 'clouds' in ww.lower():
                ww = 'clouds'
            else:
                continue

            if len(hh) and len(pp) and len(tt) and len(ww):
                merged_data.write(f'{hh},{pp},{tt},{ww}\n')
    
    merged_data.close()
    return True


def show_data(input_file: str) -> None:
    with open(input_file, 'r', 1, 'utf8') as f:
        fig = plt.figure()
        ax = fig.add_subplot(projection='3d')
        ax.set_xlabel('Humidity')
        ax.set_ylabel('Pressure')
        ax.set_zlabel('Temperature')

        cnt = 0
        for line in f.readlines():
            if cnt > 10000: break
            x, y, z, weather = line.split(',')
            weather = weather.strip()

            c = 'black'
            if weather == 'rain':
                c = 'blue'
            elif weather == 'snow':
                c = 'black'
            elif weather == 'clear':
                c = 'green'
            else:
                c = 'gray'

            ax.scatter(float(x), float(y), float(z), c=c)
            cnt += 1

        plt.show()


if __name__ == '__main__':
    # merge_datasets()
    show_data(os.path.join(MERGED_DATA_PATH, 'data.csv'))