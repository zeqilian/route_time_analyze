#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

import click
import datetime

import pandas as pd

from record_time_classify import classify_vehicle_by_time_detail

pd.set_option('display.width', 1000)


@click.group()
def print_data_tools():
    pass


@print_data_tools.command()
@click.argument('route')
def route_data(route):
    df = pd.read_csv('record_data.csv')
    click.echo('---------record_data.csv---------')
    click.echo(df[df.route == route])

    df = pd.read_csv('res.csv')
    click.echo('---------res.csv---------')
    click.echo(df[df.route == route])

    df = pd.read_csv('new_res.csv')
    click.echo('---------new_res.csv---------')
    click.echo(df[df.route == route])


@print_data_tools.command()
@click.argument('record_id', type=int)
def record_data(record_id):
    df = pd.read_csv('record_data.csv')
    click.echo(df[df.id == record_id])


@print_data_tools.command()
def go():
    df = pd.read_csv('record_data.csv')
    df = df[(df.date >= 20200516) & (df.date < 20200615)]
    #click.echo(df[df.time_cost <= 60])
    click.echo(df.groupby('vc_trip_id').count().count())


@print_data_tools.command()
def vehicle_by_time():
    df = pd.read_csv('res.csv')
    #df = df[df.duration_sec >= 15 * 60]
    click.echo(df)

    now = datetime.date.today()
    #time_list = [now - datetime.timedelta(days=n) for n in (0, 30, 60, 90)]
    #time_list = [int(d.strftime('%Y%m%d')) for d in time_list]
    time_list = [20200615, 20200516]

    for i in range(len(time_list) - 1):
        begin_date = time_list[i + 1]
        end_date = time_list[i]
        date_df = df.copy()[(df.date >= begin_date) & (df.date < end_date)]
        print('record count', date_df.count())
        name = '{}-{}'.format(begin_date, end_date)
        click.echo(name)
        #click.echo(date_df)
        date_df = classify_vehicle_by_time_detail(date_df)
        date_df = date_df.fillna(0)
        date_df.to_csv(name + '.csv', index=False)
        click.echo(date_df.to_csv(index=False, sep='\t'))


if __name__ == '__main__':
    print_data_tools()
