'''
Author: Warren Liu
Date: 11/30/2022
'''

import os

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
            if len(hh) and len(pp) and len(tt) and len(ww):
                merged_data.write(f'{hh},{pp},{tt},{ww}\n')


if __name__ == '__main__':
    merge_datasets()