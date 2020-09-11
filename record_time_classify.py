#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

import datetime

import pandas as pd
from collections import OrderedDict

pd.set_option('display.width', 1000)

time_ranges = (
    ('10:30', '12:30'),
    ('13:30', '15:30'),
    ('16:00', '18:00'),
)

time_slot_whole_day = (
    ('00:00', '06:00'),
    ('06:00', '10:00'),
    ('10:00', '14:00'),
    ('14:00', '18:00'),
    ('18:00', '22:00'),
    ('22:00', '24:00'),
)

def get_time_slot_tz(tz, slots):
    dt = datetime.datetime.fromtimestamp(tz)
    dt = dt.strftime('%H:%M')
    for tr in slots:
        if tr[0] <= dt < tr[1]:
            return '{}-{}'.format(*tr)

def add_time_slot_col(df, field_name, slots):
    df['time_slot'] = df.apply(lambda row: get_time_slot_tz(row[field_name], slots), axis=1)
    return df

def get_time_range(row):
    res = []
    start_time = row['start_time']
    end_time = row['end_time']
    for tr in time_ranges:
        if start_time <= tr[1] and end_time >= tr[0]:
            res.append('{}-{}'.format(*tr))

    if res:
        return '|'.join(res)
    #return res[0] return None


def format_time(seconds):
    return '{}h{}m'.format(int(seconds / 3600), int(seconds % 3600 / 60))


def format_df(df):
    time_cols = ['max_duration', 'median_duration', 'min_duration']
    for col in time_cols:
        df[col] = df[col].apply(format_time)


def classify_route_by_time(df, for_test=False):
    df['time_range'] = df.apply(get_time_range, axis=1)
    s = df['time_range'].str.split('|').apply(pd.Series, 1).stack()
    s.index = s.index.droplevel(-1)
    s.name = 'time_range'
    del df['time_range']
    df = df.join(s)

    #df = df.dropna()

    df = df.groupby(['time_range', 'route']).agg(
        OrderedDict([
            ('duration_sec', ['max', 'median', 'min']),
            ('distance', ['max', 'median', 'min']),
            ('vehicle_id', ['count']),
        ])).reset_index()

    df.columns = [
        'time_range', 'route', 'max_duration', 'median_duration', 'min_duration', 'max_distance',
        'median_distance', 'min_distance', 'route_count'
    ]
    if for_test:
        print(df)

    df = df[df.route_count >= 3]
    time_route_df = df.sort_values(by=['time_range', 'median_duration'], ascending=[True, False])

    format_df(time_route_df)

    if for_test:
        print(time_route_df)

    time_route_df = time_route_df[['time_range', 'route', 'median_duration']]

    route_df = df.sort_values(by=['route', 'median_duration'], ascending=[True, True])
    route_df = route_df[['route', 'time_range'] + list(route_df.columns)[2:]]

    format_df(route_df)

    if for_test:
        print(route_df)

    if for_test:
        time_route_df.to_csv('time_route_data.csv', index=False)
        route_df.to_csv('route_time_data.csv', index=False)

    return time_route_df, route_df


def classify_vehicle_by_time(df, for_test=True):
    df['time_range'] = df.apply(get_time_range, axis=1, reduce=True)
    #df["time_range"]=df["time_range"].str.split("|")
    #df = df.explode("Shape").reset_index(drop=True)
    s = df['time_range'].str.split('|').apply(pd.Series, 1).stack()
    s.index = s.index.droplevel(-1)
    s.name = 'time_range'
    del df['time_range']
    df = df.join(s)

    #print(df)
    #df = df.dropna()

    df = df.groupby(['date', 'time_range', 'vehicle_id']).agg({'route': 'count'}).reset_index()

    if for_test:
        print(df)

    df = df.groupby(['time_range', 'vehicle_id']).agg({
        'date': 'count'
    }).reset_index().rename(columns={
        'date': 'total_count'
    })

    if for_test:
        print(df)
    return df


def classify_vehicle_by_time_detail(df, for_test=False):
    all_df = classify_vehicle_by_time(df, for_test)
    eng_vo_df = df[(df.trip_type == 'ENGINEER_TESTING') & (df.vo_ticket.notnull())].copy()
    eng_no_vo_df = df[(df.trip_type == 'ENGINEER_TESTING') & (df.vo_ticket.isnull())].copy()
    other_df = df[df.trip_type != 'ENGINEER_TESTING'].copy()

    eng_vo_df = classify_vehicle_by_time(eng_vo_df, for_test).rename(columns={
        'total_count': 'eng_vo'
    })
    eng_no_vo_df = classify_vehicle_by_time(eng_no_vo_df,
                                            for_test).rename(columns={
                                                'total_count': 'eng_no_vo'
                                            })
    other_df = classify_vehicle_by_time(other_df, for_test).rename(columns={'total_count': 'other'})
    all_df = all_df.merge(eng_vo_df, on=['time_range', 'vehicle_id'], how='left')
    all_df = all_df.merge(eng_no_vo_df, on=['time_range', 'vehicle_id'], how='left')
    all_df = all_df.merge(other_df, on=['time_range', 'vehicle_id'], how='left')

    return all_df

def main():
    pd.set_option('display.width', 1000)
    df = pd.read_csv('new_res.csv')
    time_route_df, route_df = classify_route_by_time(df, True)
    #df = pd.read_csv('res.csv')
    #classify_vehicle_by_time(df, True)

if __name__ == '__main__':
    main()
