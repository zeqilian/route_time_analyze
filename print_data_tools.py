#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright @2020 Pony AI Inc. All rights reserved.
# Author: zeqilian@pony.ai (Jacky Lian)

import click
import pandas as pd

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
  click.echo(df[df.time_cost <= 60])

if __name__ == '__main__':
  print_data_tools()
