import pandas as pd

import sqlalchemy as db


def load_data_from_engeto():
    user = "student"
    password = "p7@vw7MCatmnKjy7"
    conn_string = f"mysql+pymysql://{user}:{password}@data.engeto.com/data"
    engeto_conn = db.create_engine(conn_string, echo=True)

    db_connection = engeto_conn.connect()

    bikes_df = pd.read_sql_query(
        "select * from edinburgh_bikes limit 100",
        engeto_conn, parse_dates=True)
    weather_df = pd.read_sql(
        'select * from edinburgh_weather limit 100',
        engeto_conn, parse_dates=True)

    db_connection.close()

    return bikes_df, weather_df


def write_data_to_csv():
    pass


def data_description(bike_df, weather_df):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    print('=' * 50)
    print(bike_df.columns)
    print(bike_df.describe())
    print(bike_df.head(10))
    print('=' * 50)
    print(weather_df.columns)
    print(weather_df.describe())
    print(weather_df.head(10))


def main():
    bikes_df, weather_df = load_data_from_engeto()
    data_description(bikes_df, weather_df)


if __name__ == '__main__':
    main()
