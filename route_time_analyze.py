#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

import pandas as pd

from record_join import join_record_route
from record_filter import filter_uncomplete_records
from record_time_classify import classify_route_by_time

if __name__ == "__main__":
  #df = pd.read_csv('record_data.csv')
  df = pd.read_csv('record_2020-06-15.csv')
  df = join_record_route(df)
  df = filter_uncomplete_records(df)
  time_route_df, route_df = classify_route_by_time(df)
  time_route_df.to_csv('time_route_data.csv', index=False)
  route_df.to_csv('route_time_data.csv', index=False)
