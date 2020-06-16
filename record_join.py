#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

from datetime import datetime

import pandas as pd


class Route(object):

  def __init__(self, record_id='', date_str='', time_str='', distance=0, last_route=None, **kwargs):
    self.record_id = record_id
    self.date_str = date_str
    if not date_str or time_str or not last_route or last_route.record_id == record_id:
      self.start_timestamp = 0
    else:
      self.start_timestamp = self.get_timestamp(date_str, time_str)
    self.end_timestamp = 0
    self.route = ''
    self.distance = 0
    self.ext_keys = kwargs.keys()
    for key, value in kwargs.items():
      self.__setattr__(key, value)

  def is_same_route(self, begin_time, route):
    if begin_time - self.end_timestamp > 3600:
      return False
    if begin_time < self.end_timestamp:
      return False
    if self.route and route and self.route != route:
      return False
    return True

  def update_info(self, start_time, end_time, route, distance):
    if self.start_timestamp == 0 or start_time < self.start_timestamp:
      self.start_timestamp = start_time
    if self.end_timestamp == 0 or end_time > self.end_timestamp:
      self.end_timestamp = end_time
    if not self.route:
      self.route = route
    self.distance += distance

  @property
  def date(self):
    return self.date_str

  @classmethod
  def get_timestamp(cls, date_str, time_str):
    dt_str = '{:08}{:06}'.format(int(date_str), int(time_str))
    return int(datetime.strptime(dt_str, '%Y%m%d%H%M%S').timestamp())

  @classmethod
  def get_time_str(cls, timestamp):
    # return '{}({})'.format(datetime.fromtimestamp(timestamp).strftime('%H:%M'), timestamp)
    return datetime.fromtimestamp(timestamp).strftime('%H:%M')

  @property
  def duration_sec(self):
    return self.end_timestamp - self.start_timestamp

  @property
  def duration(self):
    seconds = self.duration_sec
    return '{}h{}m'.format(int(seconds/3600), int(seconds % 3600/60))

  @property
  def start_time(self):
    return self.get_time_str(self.start_timestamp)

  @property
  def end_time(self):
    return self.get_time_str(self.end_timestamp)

  def get_df_row(self, header):
    return [self.__getattribute__(key) for key in header]


def map_same_route(route):
  same_route_map = {
      'garage_dongguanshatiandong': 'garage_to_dongguangshatiandong',
      'garage_dongguan_changping': 'garage_to_dongguanchangping',
      'garage_miaobeisha': 'garage_to_miaobeisha',
      'toyota_shunde': '',
      'toyota_huangge': '',
      'garage_miaobeisha_huanglan': '',
      'foshan': '',
      'miaobeisha_huanglan_small_u_turn': '',
      'small_u_turn': '',
      'huanglan': '',
      'miaobeisha': '',
      'nansha_port': '',
      'dongguanshatiandong': '',
      'small_loop': '',
      'dongguanshatian': '',
      'nansha_port_left_turn': '',
  }
  return same_route_map.get(route, route)


def join_record_route(df, for_test=False):
  df = df[df.time_cost > 60]
  df = df.fillna('')
  routes = {}
  for _, row in df.iterrows():
    route = map_same_route(row.route)
    if not route:
      continue
    if row.vehicle_id not in routes:
      routes[row.vehicle_id] = []
      last_route = Route()
    else:
      last_route = routes[row.vehicle_id][-1]
    if not last_route.is_same_route(row.start_timestamp, route):
      ext_keys = ['vehicle_id', 'vo_ticket', 'trip_type']
      ext_dict = dict([(key, row[key]) for key in ext_keys])
      last_route = Route(row.id, row.date, row.time, row.distance, last_route, 
          **ext_dict)
      routes[row.vehicle_id].append(last_route)
    last_route.update_info(row.start_timestamp, row.end_timestamp, route, row.distance)

  header = ['date', 'start_time', 'end_time',
            'route', 'duration', 'duration_sec', 'distance'] + ext_keys
  route_list = []
  for one_route_list in routes.values():
    for route in one_route_list:
      route_list.append(route.get_df_row(header))

  df = pd.DataFrame(route_list, columns=header)

  if for_test:
    print(df)
    df.to_csv('res.csv', index=False)

  return df


if __name__ == '__main__':
  # print(Route.get_timestamp('20200604', '092518'))
  pd.set_option('display.width', 1000)
  df = pd.read_csv('record_data.csv')
  df = join_record_route(df, True)
