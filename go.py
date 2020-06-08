#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

from datetime import datetime


class Route(object):

  def __init__(self, record_id='', vehicle_id='', date_str='', time_str='', distance = 0, last_route=None):
    self.record_id = record_id
    self.vehicle_id = vehicle_id
    self.date_str = date_str
    if not date_str or time_str or not last_route or last_route.record_id == record_id:
      self.start_time = 0
    else:
      self.start_time = self.get_timestamp(date_str, time_str)
    self.end_time = 0
    self.route = ''
    self.distance = 0

  def is_same_route(self, begin_time, route):
    if begin_time - self.end_time > 3600:
      return False
    if begin_time < self.end_time:
      return False
    if self.route and route and self.route != route:
      return False

    return True

  def update_info(self, start_time, end_time, route, distance):
    if self.start_time == 0 or start_time < self.start_time:
      self.start_time = start_time
    if self.end_time == 0 or end_time > self.end_time:
      self.end_time = end_time
    if not self.route:
      self.route = route
    self.distance += distance

  @classmethod
  def get_timestamp(cls, date_str, time_str):
    dt_str = '{:08}{:06}'.format(int(date_str), int(time_str))
    return int(datetime.strptime(dt_str, '%Y%m%d%H%M%S').timestamp())

  @classmethod
  def get_time_str(cls, timestamp):
    # return '{}({})'.format(datetime.fromtimestamp(timestamp).strftime('%H:%M'), timestamp)
    return datetime.fromtimestamp(timestamp).strftime('%H:%M')

  @property
  def run_time_sec(self):
    return self.end_time - self.start_time

  @property
  def run_time(self):
    seconds = self.run_time_sec
    return '{}h{}m'.format(int(seconds/3600), int(seconds % 3600/60))

  def __str__(self):
    return '{},{},{},{},{},{},{},{}'.format(
        self.date_str, self.get_time_str(self.start_time), self.get_time_str(
            self.end_time), self.vehicle_id, self.route,
        self.run_time, self.run_time_sec, self.distance)


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


def go():
  routes = {}
  for line in open('record_data.csv'):
    record_id, vc_trip_id, date_str, vehicle_id, time_str, route, vehicle_mode, total_distance, total_time, total_auto_time, distance, time_cost, start_timestamp, end_timestamp = line.strip(
    ).split(',')
    route = map_same_route(route)
    if not route:
        continue
    if record_id == 'id':
      continue
    if vehicle_id not in routes:
      routes[vehicle_id] = []
      last_route = Route()
    else:
      last_route = routes[vehicle_id][-1]
    if not last_route.is_same_route(int(start_timestamp), route):
      last_route = Route(record_id, vehicle_id, date_str, time_str, distance, last_route)
      routes[vehicle_id].append(last_route)
    last_route.update_info(int(start_timestamp), int(end_timestamp), route, int(distance))

  return routes


def print_routes(routes):
  print('date,start_time,end_time,vehicle_id,route,duration,duration_sec,distance')
  for vehicle_id, route_list in routes.items():
    for route in route_list:
      print(route)


if __name__ == '__main__':
  #print(Route.get_timestamp('20200604', '092518'))
  routes = go()
  print_routes(routes)