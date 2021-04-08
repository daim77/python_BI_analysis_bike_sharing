import datetime
import pandas as pd

import sqlalchemy as db


def load_data_from_engeto():
    user = "student"
    password = "p7@vw7MCatmnKjy7"
    conn_string = f"mysql+pymysql://{user}:{password}@data.engeto.com/data"
    engeto_conn = db.create_engine(conn_string, echo=True)

    db_connection = engeto_conn.connect()

    bikes_df = pd.read_sql_query(
        "SELECT "
        "started_at, ended_at, "
        "start_station_id, start_station_latitude, start_station_longitude, "
        "end_station_id, end_station_latitude, end_station_longitude "
        "FROM edinburgh_bikes;",
        engeto_conn, parse_dates=True
    )

    weather_df = pd.read_sql_query(
        "SELECT "
        "date, time, temp, feels, wind, gust, rain, humidity, cloud, vis "
        "FROM edinburgh_weather;",
        engeto_conn, parse_dates=True)

    db_connection.close()

    weather_df.drop(weather_df.loc[weather_df['date'] < '2018-09-15'].index,
                    inplace=True)
    weather_df.drop(weather_df.loc[weather_df['date'] == '2020-10-31'].index,
                    inplace=True)
    write_data_to_csv(bikes_df, weather_df)

    user = "lamikoko.cz.2"
    password = "Zzdenka1"
    conn_string = f"mysql+pymysql://{user}:{password}@lamikoko.cz/lamikokocz2"

    lamikoko_conn = db.create_engine(conn_string, echo=True)
    bikes_df.to_sql('bikes', lamikoko_conn)
    weather_df.to_sql('weather', lamikoko_conn)

    return bikes_df, weather_df


def load_from_lamikoko():
    user = "lamikoko.cz.2"
    password = "Zzdenka1"
    conn_string = f"mysql+pymysql://{user}:{password}@lamikoko.cz/lamikokocz2"

    lamikoko_conn = db.create_engine(conn_string, echo=True)
    bikes_df = pd.read_sql('bikes', lamikoko_conn, parse_dates=True)
    weather_df = pd.read_sql('weather', lamikoko_conn, parse_dates=True)

    return bikes_df, weather_df


def write_data_to_csv(bikes_df, weather_df):
    bikes_df.to_csv('tables/bikes.csv', sep='\t')
    weather_df.to_csv('tables/weather.csv', sep='\t')
    return


def data_stat(bikes_df, weather_df):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    print('=' * 50)
    print(bikes_df.columns)
    print(bikes_df.head(10))
    print('=' * 50)
    print(weather_df.columns)
    print(weather_df.head(10))

    stations_starts_df = bikes_df['start_station_id'].value_counts()
    stations_ends_df = bikes_df['end_station_id'].value_counts()

    print('Nejvice se akumuluje: ',
          (stations_ends_df - stations_starts_df).idxmax())

    print('Number of stations',
          stations_starts_df.index.__len__())
    print('Station used less then 50 times',
          stations_starts_df.loc[stations_starts_df < 50].index.__len__())

    print(bikes_df.started_at.min())
    print(bikes_df.started_at.max())


def compute_data(bikes_df, weather_df):
    bikes_df['duration'] = bikes_df['ended_at'] - bikes_df['started_at']
    bikes_df['duration'] = bikes_df['duration'].dt.seconds
    return


def bikes_movement(bikes_df):
    date_transfer = bikes_df.started_at.dt.date.min()
    period_df = bikes_df.started_at.dt.date.max() \
                - bikes_df.started_at.dt.date.min()
    period = period_df.days
    for i in range(1, period + 1):

        mov_start_df = \
            bikes_df.loc[bikes_df['started_at']
                             .dt.date <= date_transfer, ['start_station_id']]\
                .value_counts()

        mov_end_df = \
            bikes_df.loc[bikes_df['started_at'].
                             dt.date <= date_transfer, ['end_station_id']]\
                .value_counts()

        diff_df = mov_end_df - mov_start_df

        if diff_df.max() > 10:
            print('diff_df_max: ', diff_df.idxmax)

        date_transfer += datetime.timedelta(days=i)

    return


def main():
    # bikes_df, weather_df = load_data_from_engeto()
    bikes_df, weather_df = load_from_lamikoko()

    data_stat(bikes_df, weather_df)

    compute_data(bikes_df, weather_df)


if __name__ == '__main__':
    main()
