#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='zones', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc'
    url = f'{prefix}/taxi_zone_lookup.csv'

    print("Reading zones dataset...")
    df_zones = pd.read_csv(url)

    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    print("Ingesting zones data into PostgreSQL...")
    df_zones.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace'
    )

    print("Zones table successfully created!")


if __name__ == "__main__":
    main()