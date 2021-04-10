import datetime
import requests

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
    return bikes_df, weather_df


def data_to_lamikoko(bikes_df, weather_df):
    user = "lamikoko.cz.2"
    password = "Zzdenka1"
    conn_string = f"mysql+pymysql://{user}:{password}@lamikoko.cz/lamikokocz2"

    lamikoko_conn = db.create_engine(conn_string, echo=True)
    bikes_df.to_sql('bikes', lamikoko_conn)
    weather_df.to_sql('weather', lamikoko_conn)

    return


def load_from_lamikoko():
    user = "lamikoko.cz.2"
    password = "Zzdenka1"
    conn_string = f"mysql+pymysql://{user}:{password}@lamikoko.cz/lamikokocz2"

    lamikoko_conn = db.create_engine(conn_string, echo=True)
    bikes_df = pd.read_sql('bikes', lamikoko_conn, parse_dates=True)
    weather_df = pd.read_sql('weather', lamikoko_conn, parse_dates=True)

    return bikes_df, weather_df


def data_prep(weather_df):
    weather_df['wind_speed_km_h'] = weather_df['wind'].str.split(' ').str[0]
    weather_df['wind_direction'] = weather_df['wind'].str.split(' ').str[3]
    weather_df['gust_km_h'] = weather_df['gust'].str.split(' ').str[0]
    weather_df['temp_c'] = weather_df['temp'].str.split(' ').str[0]
    weather_df['feels_c'] = weather_df['feels'].str.split(' ').str[0]
    weather_df['rain_mm'] = weather_df['rain'].str.split(' ').str[0]
    weather_df['humidity_%'] = weather_df['humidity'].str.rstrip('%')
    weather_df['cloudiness_%'] = weather_df['cloud'].str.rstrip('%')

    weather_df = weather_df.astype({'wind_speed_km_h': int,
                                    "gust_km_h": int,
                                    'temp_c': int,
                                    'feels_c': int,
                                    'rain_mm': float,
                                    'humidity_%': int,
                                    'cloudiness_%': int, })
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    weather_df.drop(
        ['wind', 'gust', 'temp', 'feels', 'rain', 'humidity', 'cloud'], axis=1,
        inplace=True)

    weather_df['wind_direction'] = weather_df['wind_direction'].map(
        {'S': 180, 'SSW': 202.5, 'SW': 225, 'SE': 135,
         'WSW': 247.5, 'W': 270, 'NE': 45, 'ENE': 67.5,
         'E': 90, 'NNE': 22.5, 'NNW': 337.5, 'NW': 315,
         'WNW': 292.5, 'SSE': 157.5, 'ESE': 112.5, 'N': 0})
    return weather_df


def compute_data(bikes_df, df_station_id):
    bikes_df['duration'] = bikes_df['ended_at'] - bikes_df['started_at']
    bikes_df['duration'] = bikes_df['duration'].dt.seconds
    elev_dict = pd.Series(df_station_id['elev']
                          .values,
                          index=df_station_id.index).to_dict()
    bikes_df['start_elev'] = bikes_df['start_station_id'].map(elev_dict)
    bikes_df['end_elev'] = bikes_df['end_station_id'].map(elev_dict)
    bikes_df['delta_elev'] = bikes_df['end_elev'] - bikes_df['start_elev']
    return


def data_stat(bikes_df, weather_df):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    print('=' * 50)
    print(bikes_df.columns)
    print(bikes_df.head(10))
    print(bikes_df.info)
    print('=' * 50)
    print(weather_df.columns)
    print(weather_df.head(10))
    print(weather_df.info)

    stations_starts_df = bikes_df['start_station_id'].value_counts()
    stations_ends_df = bikes_df['end_station_id'].value_counts()

    print('accumulation at: ',
          (stations_ends_df - stations_starts_df).idxmax())

    print('Number of stations: ',
          stations_ends_df.index.__len__())
    print('Station used less then 50 times: ',
          stations_starts_df.loc[stations_starts_df < 50].index.__len__())


def bikes_movement(bikes_df):
    date_transfer = bikes_df.started_at.dt.date.min()
    period_df = \
        bikes_df.started_at.dt.date.max() - bikes_df.started_at.dt.date.min()
    period = period_df.days
    for i in range(1, period + 1):

        mov_start_df = \
            bikes_df.loc[bikes_df['started_at'].dt.date <= date_transfer,
                         ['start_station_id']].value_counts()

        mov_end_df = \
            bikes_df.loc[bikes_df['started_at'].dt.date <= date_transfer,
                         ['end_station_id']].value_counts()

        diff_df = mov_end_df - mov_start_df

        if diff_df.max() > 10:
            print('diff_df_max: ', diff_df.idxmax)

        date_transfer += datetime.timedelta(days=i)
    return


def data_visual(bikes_df, weather_df):
    # wind speed and gust dependency
    df = weather_df.loc[:, ['wind_speed_km_h', 'gust_km_h']]
    df.plot.scatter('wind_speed_km_h', 'gust_km_h', figsize=(12, 6),
                    marker='x', color='blue')
    # temp and feels temp dependency
    df1 = weather_df.loc[:, ['temp_c', 'feels_c']]
    df1.plot.scatter('temp_c', 'feels_c', figsize=(12, 6), marker='x',
                     color='red')
    # start_station and end_station dependency
    df2 = bikes_df.loc[:, ['start_station_id', 'end_station_id']]
    df2.plot.scatter('start_station_id', 'end_station_id', figsize=(12, 6),
                     marker='x', color='k')
    # jurney delta elevation histogram
    df5 = bikes_df.loc[:, ['delta_elev']]
    df5.plot.hist('delta_elev', figsize=(12, 6), color='green', bins=15)
    return


def unique_stations_id(bikes_df):
    df3 = pd.DataFrame(bikes_df.loc[:,
                       ['start_station_id', 'start_station_latitude',
                        'start_station_longitude']]) \
        .drop_duplicates('start_station_id', keep='first') \
        .rename(columns={'start_station_id': 'station_id',
                         'start_station_latitude': 'lat',
                         'start_station_longitude': 'long'})

    df4 = bikes_df.loc[:, ['end_station_id', 'end_station_latitude',
                           'end_station_longitude']]\
        .drop_duplicates('end_station_id', keep='first') \
        .rename(columns={'end_station_id': 'station_id',
                         'end_station_latitude': 'lat',
                         'end_station_longitude': 'long'})
    df_stations_id = pd.merge(df4, df3, left_on='station_id',
                              right_on='station_id', how='left')
    df_stations_id = df_stations_id.drop(['lat_y', 'long_y'],
                                         axis=1).sort_index(ascending=False)
    df_stations_id = df_stations_id.rename(
        columns={'lat_x': 'lat', 'long_x': 'long'})
    return df_stations_id


def get_elevation_osm(lat, long):
    osm_api = \
        f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{long}"
    response = requests.get(osm_api)
    elevation = response.json()
    return elevation['results'][0]['elevation']


def write_data_to_csv(bikes_df, weather_df, df_station_id):
    bikes_df.to_csv('tables/bikes.csv', sep='\t')
    weather_df.to_csv('tables/weather.csv', sep='\t')
    df_station_id.to_csv('tables/df_station_id.csv', sep='\t')
    return


def main():
    bikes_df, weather_df = load_data_from_engeto()
    # data_to_lamikoko(bikes_df, weather_df)
    # bikes_df, weather_df = load_from_lamikoko()
    weather_df = data_prep(weather_df)

    df_station_id = unique_stations_id(bikes_df)
    df_station_id['elev'] = df_station_id.iloc[:] \
        .apply(lambda x: get_elevation_osm(x['lat'], x['long']), axis=1)

    compute_data(bikes_df, df_station_id)
    # bikes_movement(bikes_df)

    data_visual(bikes_df, weather_df)
    data_stat(bikes_df, weather_df)

    write_data_to_csv(bikes_df, weather_df, df_station_id)


if __name__ == '__main__':
    main()
