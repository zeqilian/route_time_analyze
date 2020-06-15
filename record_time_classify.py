#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

import pandas as pd
from collections import OrderedDict

pd.set_option('display.width', 1000)

time_ranges = (
    ('10:30', '12:30'),
    ('13:30', '15:30'),
    ('16:00', '18:00'),
)


def get_time_range(row):
  start_time = row['start_time']
  for tr in time_ranges:
    if start_time <= tr[1]:
      return '{}-{}'.format(*tr)

  return None


def format_time(seconds):
  return '{}h{}m'.format(int(seconds/3600), int(seconds % 3600/60))


def format_df(df):
  time_cols = ['max_duration', 'median_duration', 'min_duration']
  for col in time_cols:
    df[col] = df[col].apply(format_time)


def classify_by_time(df, for_test=False):
  df['time_range'] = df.apply(get_time_range, axis=1)
  df = df.dropna()

  df = df.groupby(['time_range', 'route']).agg(OrderedDict([
      ('duration_sec', ['max', 'median', 'min']),
      ('distance', ['max', 'median', 'min']),
      ('vehicle_id', ['count']),
  ])).reset_index()

  df.columns = ['time_range', 'route', 'max_duration', 'median_duration',
                'min_duration', 'max_distance', 'median_distance', 'min_distance', 'route_count']
  df = df[df.route_count >= 2]

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

  return time_route_df, route_df


if __name__ == '__main__':
  df = pd.read_csv('new_res.csv')
  time_route_df, route_df = classify_by_time(df, True)
  time_route_df.to_csv('time_route_data.csv', index=False)
  route_df.to_csv('route_time_data.csv', index=False)
