import click
import pandas as pd


@click.command()
@click.argument("day", type=int)
def main(day):
    """Simple pipeline that writes a parquet file for a given DAY."""
    click.echo(f"Running pipeline for day {day}")

    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    click.echo(df.head())

    df.to_parquet(f"output_day_{day}.parquet")


if __name__ == "__main__":
    main()