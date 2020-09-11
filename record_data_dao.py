#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Copyright @2020 Pony AI Inc. All rights reserved.
Author: zeqilian@pony.ai (Jacky Lian)
'''

import os

import pandas as pd
import sqlalchemy as sa

from common.webapps.common.server.storage.backend import database

os.environ['DATABASE_DRIVER'] = 'mysql'
os.environ['MYSQL_DATABASE'] = 'record_metric_v2'
os.environ['MYSQL_HOST'] = 'mysql.gz.corp.pony.ai'
os.environ['MYSQL_USER'] = 'record_metric_v2_ro'
os.environ['MYSQL_PASSWORD'] = 'RQyxq9KY7CT1lIuqMvPs'

Database = database.Database

Database.init()

def get_data_from_db(**kwargs):
    sql_args = {"count": 5000}
    sql_args.update(kwargs)

    sql = sa.text("select \
      record.id, vc_trip_id, date, vehicle_id, time, route, vehicle_mode, \
        FLOOR(total_distance) as total_distance, \
        FLOOR(total_time) as total_time, \
        FLOOR(total_auto_time) as total_auto_time, \
        FLOOR(distance) as distance, \
        FLOOR(end_timestamp-start_timestamp) as time_cost, \
        FLOOR(start_timestamp) as start_timestamp, \
        FLOOR(end_timestamp) as end_timestamp, \
        vo_ticket, trip_type, co_driver, safety_driver \
      from record left join route_interval on record.id=route_interval.record_id \
      where map_name='cybertron' \
      order by date desc, time asc, start_timestamp asc \
      limit :count\
      ;").bindparams(**sql_args)

    result = Database.get_session().execute(sql)
    rows = list(result)
    df = pd.DataFrame(rows, columns=result.keys())
    return df


def main():
    pd.set_option('display.width', 1000)
    df = get_data_from_db(count=5000)
    print(df.head(5))
    df.to_csv('record_data.csv', index=False)

if __name__ == '__main__':
    main()
