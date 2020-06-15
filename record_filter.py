#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

import pandas as pd

def filter_uncomplete_records(route_df, for_test=False):

  unique_routes = route_df['route'].unique()
  if for_test:
    for route in unique_routes:
        print(route)
  
  if for_test:
    print('raw df_count', len(route_df))
  route_df = route_df[route_df.duration_sec >= 15*60]
  
  if for_test:
    print('after filter short duration df_count', len(route_df))
  
  df = route_df.groupby(['route']).agg({
      'duration_sec': ['max', 'median', 'min'],
      'distance': ['max', 'median', 'min'],
      'route': 'count',
  }).reset_index()
  
  if for_test:
    print(df)
  
  md_df = route_df.groupby(['route']).distance.median().reset_index().rename(columns={'distance': 'median_distance'})
  
  df = route_df.merge(md_df, on='route', how='left')
  df = df[df.distance >= df.median_distance * 0.2]
  #df.iloc[:,0:-1].to_csv('new_res.csv', index=False)
  #print(df)
  

  if for_test:
    print('after filter lower df_count', len(df))
  
  df = df.iloc[:,0:-1]
  md_df = df.groupby(['route']).agg({'distance':'median', 'vehicle_id': 'count'}).rename(columns={'distance': 'median_distance', 'vehicle_id': 'route_count'}).reset_index()
  
  df = df.merge(md_df, on='route', how='left')
  df = df[df.distance <= df.median_distance * 2]
  #df = df[df.route_count >= 3]
  
  if for_test:
    print('after filter upper df_count', len(df))
  new_df = df.iloc[:,0:-2]
  
  df = df.groupby(['route']).agg({
      'duration_sec': ['max', 'median', 'min'],
      'distance': ['max', 'median', 'min'],
      'route': 'count',
  }).reset_index()
  
  if for_test:
    print(df)
  return new_df

#print(df)
if __name__ == '__main__':
  pd.set_option('display.width', 1000)
  route_df = pd.read_csv('res.csv')
  df = filter_uncomplete_records(route_df, True)
  df.to_csv('new_res.csv', index=False)
