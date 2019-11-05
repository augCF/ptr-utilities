import psycopg2
import csv
import os
import datetime
import pandas as pd
import numpy as np
import time


class InitTools:
    def __init__(self):
        self.conn = self.create_connection()
        self.cursor = self.conn.cursor()

    def create_initial_table(self, indexing=True):
        '''
        Creates initial Table and Columns of databased. These are used in this Project. Shall be executed only
        one time. Prerequisite for further work with database. Requires set up Database called Forex_Data which was
        created in PGAdmin using in PGadmin Query Tool:
        CREATE DATABASE "Forex_Data"
        WITH
        OWNER = postgres
        ENCODING = 'UTF8'
        CONNECTION LIMIT = -1;
        :return: initial Tables in Database needed for this project
        '''
        print("setting up initial tables..")
        try:
            self.cursor.execute(""" CREATE TABLE public.fx_data
                                    (
                                        fx_timestamp_id integer,
                                        fx_timestamp character varying(15),
                                        fx_year smallint,
                                        fx_month smallint,
                                        fx_day smallint,
                                        fx_weekday smallint,
                                        fx_hour smallint,
                                        fx_minute smallint
                                    )
                                    WITH (
                                        OIDS = FALSE
                                    );

                                    ALTER TABLE public.fx_data
                                        OWNER to postgres;
                                    ALTER TABLE public.fx_data
                                        ADD PRIMARY KEY (fx_timestamp_id);
                                    """)
            self.conn.commit()
            if indexing == True:
                self.cursor.execute(""" CREATE INDEX idx_fx_timestamp_id on fx_data ("fx_timestamp_id");
                                        CREATE INDEX idx_fx_timestamp on fx_data ("fx_timestamp");
                                        CREATE INDEX idx_fx_year on fx_data ("fx_year");
                                        CREATE INDEX idx_fx_month on fx_data ("fx_month");
                                        CREATE INDEX idx_fx_day on fx_data ("fx_day");
                                        CREATE INDEX idx_fx_hour on fx_data ("fx_hour");
                                        CREATE INDEX idx_fx_minute on fx_data ("fx_minute");
                                    """)
                self.conn.commit()
                print("table set with indexes.")
            else:
                print("table set.")
        except:
            print("error setting up tables.")

    def create_connection(self):
        '''
        Simple login to database with psycopg2. Change settings in this function if needed.
        :return: psycopg2 connection object to create cursor from and commit.
        '''
        return psycopg2.connect(
            host="127.0.0.1",
            dbname="Forex_Data",
            user="postgres",
            password="postgres",
            port="5432")

    def timestamp_fill(self, start_year=2000, last_year=2001, commit_batch_size=50000):
        '''
        Fills fx_timestamp and fx_timestamp_id column of database. Prerequisite for further work with database.
        Requires set up Database with initial tables/columns (use create_initial_tables). Shall be executed only one
        time. Set start_year and last_year with enough space for extension, eg. last_year = 2025.
        :param start_year: first years
        :param last_year: last year to be filled
        :return: filled database
        '''

        if start_year > last_year:
            print("error, start year must be smaller or equal to last year")

        else:
            # set list with range of years
            years = []
            for i in range(start_year, last_year + 1):
                years.append(str(i))

            # set list of months (1-12)
            months = []
            for i in range(1, 13):
                if i < 10:
                    months.append("0" + str(i))
                else:
                    months.append(str(i))

            # set list of all possible days (1-31)
            days = []
            for i in range(1, 32):
                if i < 10:
                    days.append("0" + str(i))
                else:
                    days.append(str(i))

            # set list of all hours (0-23)
            hours = []
            for i in range(0, 24):
                if i < 10:
                    hours.append("0" + str(i))
                else:
                    hours.append(str(i))

            # set list of all minutes (0-59)
            minutes = []
            for i in range(0, 60):
                if i < 10:
                    minutes.append("0" + str(i))
                else:
                    minutes.append(str(i))

            try:
                print("start creating timestamps in database...")
                i = 1
                for year in years:
                    for month in months:
                        for day in days:

                            try:
                                weekday = datetime.datetime(int(year), int(month), int(day))
                                weekday = int(datetime.datetime.weekday(weekday))
                            except:
                                weekday = "NULL"

                            for hour in hours:
                                for minute in minutes:
                                    self.cursor.execute(f"""
                                                INSERT INTO fx_data
                                                        (fx_timestamp_id, 
                                                         fx_timestamp, 
                                                         fx_year,
                                                         fx_month,
                                                         fx_day,
                                                         fx_weekday,
                                                         fx_hour,
                                                         fx_minute)
                                                VALUES
                                                        ({i}, 
                                                         '{year}{month}{day} {hour}{minute}00',
                                                         {int(year)}, 
                                                         {int(month)}, 
                                                         {int(day)},
                                                         {weekday}, 
                                                         {int(hour)},
                                                         {int(minute)})"""
                                                        )

                                    i += 1
                                    if i % commit_batch_size == 0:
                                        self.conn.commit()
                                    if i % 100000 == 0:
                                        print(f"{year}/{month}. {i} timestamps added. ")
                self.conn.commit()
                print(f"finished. {i - 1} timestamps added. Updating indexes now...")
                self.cursor.execute("REINDEX TABLE fx_data;")
                self.conn.commit()
                print("indexes successfully updated.")
                print("Initial procedure finished.")
            except:
                print("error adding timestamps")

    def create_forex_columns(self, pair_name, indexing=True):
        '''
        Creates the tables required for filling raw data of specified forex pair in database.
        :param pair_name: Name of forex pair. must be string of 6 chars, eg. "eurusd".
        :param indexing: Boolean, if true, index on new columns is added. Default true.
        :return: new tables in database to be filled with raw forex data.
        '''

        print(f"adding Columns for {pair_name}..")
        self.cursor.execute(f"""ALTER TABLE fx_data
                                ADD COLUMN {pair_name}_bidopen NUMERIC(6,5),
                                ADD COLUMN {pair_name}_bidhigh NUMERIC(6,5),
                                ADD COLUMN {pair_name}_bidlow NUMERIC(6,5),
                                ADD COLUMN {pair_name}_bidclose NUMERIC(6,5),
                                ADD COLUMN {pair_name}_nonvalid_count integer,
                                ADD COLUMN {pair_name}_valid_id integer;""")
        self.conn.commit()
        print("Columns added.")
        if indexing == True:
            print(f"adding Indexes on {pair_name} tables..")
            self.cursor.execute(
                f"""CREATE INDEX idx_{pair_name}_bidopen on fx_data ("{pair_name}_bidopen");
                    CREATE INDEX idx_{pair_name}_bidhigh on fx_data ("{pair_name}_bidhigh");
                    CREATE INDEX idx_{pair_name}_bidlow on fx_data ("{pair_name}_bidlow");
                    CREATE INDEX idx_{pair_name}_bidclose on fx_data ("{pair_name}_bidclose");
                    CREATE INDEX idx_{pair_name}_nonvalid_count on fx_data ("{pair_name}_nonvalid_count");
                    CREATE INDEX idx_{pair_name}_valid_id on fx_data ("{pair_name}_valid_id");""")
            self.conn.commit()
            print("Indexes added.")

    def update_raw_fx_data(self, pairname, csv_folder, commit_batch_size=25000):
        '''
        Inserts all Data from a given (set of) csv(s) in specified folder. CSV Data must have format like Histdata.
        :param pairname: Name of forex pair. must be string of 6 chars, eg. "eurusd".
        :param csv_folder: Filepath to folder where csv(s) with mentioned data is stored. Replace all "\" with "//"
                            and delete final "//". Eg: 'C://Users//howard//datasets//eurusd'
        :param commit_batch_size: Number of ohlc-lines to be stored before commiting to database, less commits -> faster.
        :return: Creates forex pair related columns & fills them with content from csv(s).
        '''

        print(f"Start adding files from directory {csv_folder} as {pairname} values..")
        files = os.listdir(csv_folder)
        for file in files:
            print(f"file {file} is being added..")
            filepath = csv_folder + "//" + file
            with open(filepath, mode="r") as open_file:
                read_csv = csv.reader(open_file, delimiter=";")
                i = 0
                for line in read_csv:
                    try:
                        self.cursor.execute(f"""UPDATE fx_data SET
                                                {pairname}_bidopen = {np.around(float(line[1]), decimals=5)},
                                                {pairname}_bidhigh = {np.around(float(line[2]), decimals=5)},
                                                {pairname}_bidlow = {np.around(float(line[3]), decimals=5)},
                                                {pairname}_bidclose = {np.around(float(line[4]), decimals=5)},
                                                {pairname}_nonvalid_count = 0
                                                WHERE
                                                fx_timestamp = '{line[0]}'""")
                    except:
                        print(f"{file} failure at {line[0]}; {line[1]}")

                    i += 1
                    if i % commit_batch_size == 0:
                        self.conn.commit()
                    if i % 100000 == 0:
                        print(f"{i} ohlc-lines added..")

                self.conn.commit()
                print(f"file {file} finished.")
        print(f"All files from {csv_folder} added as {pairname} values.")
        print("reindexing...")
        self.cursor.execute("REINDEX TABLE fx_data")
        print("reindexing finished.")

    def fill_empty_fx_rows(self, pairname, batchsize):
        '''

        :param pairname:
        :return:
        '''
        print(f"Start filling empty fx rows of {pairname}..")
        # start point determination, 'first row of where data acutally is', lower limit
        self.cursor.execute(f"""SELECT fx_timestamp_id FROM fx_data WHERE {pairname}_bidopen IS NOT NULL
                                ORDER BY fx_timestamp_id ASC LIMIT 1""")
        lower_limit = self.cursor.fetchall()[0][0]  # -> Because fetchall outputs sth. like this: [(blabla,)]

        # end point determination, 'last row where data actually is', upper limit
        self.cursor.execute(f"""SELECT fx_timestamp_id FROM fx_data WHERE {pairname}_bidopen IS NOT NULL
                                ORDER BY fx_timestamp_id DESC LIMIT 1""")
        upper_limit = self.cursor.fetchall()[0][0]  # -> Because fetchall outputs sth. like this: [(blabla,)]

        finished = False
        counter = 0
        while finished == False:

            # mid point determination, 'last row before first empty row'
            self.cursor.execute(f"""SELECT fx_timestamp_id FROM fx_data
                                    WHERE fx_timestamp_id > {lower_limit}
                                    AND {pairname}_bidopen IS NULL
                                    ORDER BY fx_timestamp_id ASC LIMIT 1""")
            current_id = self.cursor.fetchall()[0][0] - 1

            self.cursor.execute(f"""SELECT {pairname}_nonvalid_count FROM fx_data 
                                    WHERE fx_timestamp_id = {current_id}""")
            last_nonvalid_count_number = self.cursor.fetchall()[0][0]

            if upper_limit - current_id <= batchsize:
                query = f"SELECT fx_timestamp_id, {pairname}_bidclose, {pairname}_nonvalid_count FROM fx_data " \
                        f"WHERE fx_timestamp_id BETWEEN {current_id} AND {upper_limit}" \
                        f"ORDER BY fx_timestamp_id ASC"
            else:
                query = f"SELECT fx_timestamp_id, {pairname}_bidclose, {pairname}_nonvalid_count FROM fx_data " \
                        f"WHERE fx_timestamp_id BETWEEN {current_id} AND {current_id + batchsize}" \
                        f"ORDER BY fx_timestamp_id ASC"

            fetch = pd.read_sql_query(query, self.conn)

            nan_count = last_nonvalid_count_number  # MUST BE FIXED!!
            nan_count = 0

            for i in range(len(fetch)):
                if np.isnan(fetch.at[i, f"{pairname}_bidclose"]):
                    nan_count += 1
                    self.cursor.execute(f"""UPDATE fx_data SET 
                                            {pairname}_bidopen = {fetch.at[i - nan_count, f"{pairname}_bidclose"]},
                                            {pairname}_bidhigh = {fetch.at[i - nan_count, f"{pairname}_bidclose"]},
                                            {pairname}_bidlow = {fetch.at[i - nan_count, f"{pairname}_bidclose"]},
                                            {pairname}_bidclose = {fetch.at[i - nan_count, f"{pairname}_bidclose"]},
                                            {pairname}_nonvalid_count = {nan_count}
                                            WHERE fx_timestamp_id = {fetch.at[i, "fx_timestamp_id"]}""")
                    counter += 1
                else:
                    nan_count = 0

                if current_id == upper_limit:
                    finished = True
                    break
            print(f"current id: {current_id}. {upper_limit - current_id} left.")
            self.conn.commit()
        print(
            f"Fill empty rows procedure completed. {counter} empty {pairname} rows detected and filled with last bidclose.")

    def update_valid_id(self, pairname):
        print(f"Start updating {pairname} valid ID..")
        print(f"fetching latest ID..")
        try:
            self.cursor.execute(
                f"""SELECT {pairname}_valid_id, fx_timestamp_id FROM fx_data WHERE {pairname}_valid_id IS NOT NULL
                                ORDER BY fx_timestamp_id DESC LIMIT 1""")
            fetch = self.cursor.fetchall()
            latest_valid_id = fetch[0][0]
            latest_timestamp_id = fetch[0][1]
        except:
            self.cursor.execute(f"""SELECT fx_timestamp_id FROM fx_data WHERE {pairname}_bidopen IS NOT NULL
                                    ORDER BY fx_timestamp_id ASC LIMIT 1""")
            latest_timestamp_id = self.cursor.fetchall()[0][0]
            latest_valid_id = 0
            print(f"latest valid ID not found. starting from 0 at timestamp id {latest_timestamp_id}")

        self.cursor.execute(f"""SELECT fx_timestamp_id from fx_data WHERE fx_timestamp_id IS NOT NULL
                                ORDER BY fx_timestamp_id DESC LIMIT 1""")

        upper_limit = self.cursor.fetchall()[0][0]
        print(f"upper limit: {upper_limit}")

        self.cursor.execute(f"""SELECT fx_timestamp_id from fx_data WHERE {pairname}_bidopen IS NOT NULL
                                AND fx_timestamp_id BETWEEN {latest_timestamp_id} AND {upper_limit}
                                ORDER BY fx_timestamp_id ASC""")
        full_id_batch = np.array(self.cursor.fetchall())[:, 0]

        i = 0
        for id in full_id_batch:
            self.cursor.execute(f"""UPDATE fx_data SET {pairname}_valid_id = {latest_valid_id + i}
                                    WHERE fx_timestamp_id = {id}""")
            i += 1
            if i % 100000 == 0:
                self.conn.commit()
            if i % 25000 == 0:
                print(f"""{i} valid IDs set..""")
        self.conn.commit


class IndicatorTools:

    def __init__(self):
        self.InitToolsObject = InitTools()
        self.conn = self.InitToolsObject.create_connection()
        self.cursor = self.conn.cursor()

    def create_indicator_columns(self, pairname, indexing=True):
        '''
        Creates the coulumns for indicator values of specified forex pair in database.
        :param pairname: Name of forex pair. must be string of 6 chars, eg. "eurusd".
        :param indexing: Boolean, if true, index on new columns is added. Defaults to true.
        :return: new tables in database to be filled with indicator values.
        '''

        print(f"adding Columns for {pairname}..")
        self.cursor.execute(f"""ALTER TABLE fx_data
                                    ADD COLUMN {pairname}_up_down_1 smallint,
                                    ADD COLUMN {pairname}_absolute_body_size_1 NUMERIC(6,5),
                                    ADD COLUMN {pairname}_cutler_rsi_14 double precision,
                                    ADD COLUMN {pairname}_cutler_rsi_28 double precision,
                                    ADD COLUMN {pairname}_cutler_rsi_56 double precision,
                                    ADD COLUMN {pairname}_cutler_rsi_112 double precision,
                                    ADD COLUMN {pairname}_cutler_rsi_3000 double precision;
                                    """)
        self.conn.commit()
        print("Columns added.")

        if indexing == True:
            print(f"adding Indexes on {pairname} tables..")
            self.cursor.execute(f"""CREATE INDEX idx_{pairname}_up_down_1 on fx_data ("{pairname}_up_down_1");
                                    CREATE INDEX idx_{pairname}_absolute_body_size_1 on fx_data ("{pairname}_up_down_1");
                                    CREATE INDEX idx_{pairname}_cutler_rsi_14 on fx_data ("{pairname}_cutler_rsi_14");
                                    CREATE INDEX idx_{pairname}_cutler_rsi_28 on fx_data ("{pairname}_cutler_rsi_28");
                                    CREATE INDEX idx_{pairname}_cutler_rsi_56 on fx_data ("{pairname}_cutler_rsi_56");
                                    CREATE INDEX idx_{pairname}_cutler_rsi_112 on fx_data ("{pairname}_cutler_rsi_112");
                                    CREATE INDEX idx_{pairname}_cutler_rsi_3000 on fx_data ("{pairname}_cutler_rsi_3000");
                                """)
            self.conn.commit()
            print("Indexes added.")

    def fill_indicator_value(self, pairname, indicator, n_periods, batchsize):
        '''
        Fills respective Indicator Column. Prerequisites non-empty fx rows within start and end of valid data, use
        fill_empty_fx_rows first otherwise error will be raised.
        :param pairname: Name of forex pair. must be string of 6 chars, eg. "eurusd".
        :param indicator: String out of list of Indicators, eg. "up_down" or "rsi"
        :param windowsize: Windowsize of Indicator, eg. RSI Period. Has no effect on "up_down".
        :param batchsize: Size of Batch to be processed at once, eg. 50000 or more. Bigger is better.
        :return: Filled indicator column in Database.
        '''

        print(f"Start filling {pairname} {indicator} {n_periods} values")

        # start point determination, 'first row of where data acutally is', lower limit
        self.cursor.execute(f"""SELECT fx_timestamp_id FROM fx_data WHERE {pairname}_bidopen IS NOT NULL
                                AND {pairname}_{indicator}_{n_periods} IS NULL 
                                ORDER BY fx_timestamp_id ASC LIMIT 1""")
        lower_limit = self.cursor.fetchall()

        if lower_limit == []:  # check if there is actually sth to do
            print("already finished, nothing to fill")
            return None
        else:
            lower_limit = lower_limit[0][0]  # -> Because fetchall outputs sth. like this: [(blabla,)]

        # end point determination, 'last row where data actually is', upper limit
        self.cursor.execute(f"""SELECT fx_timestamp_id FROM fx_data WHERE {pairname}_bidopen IS NOT NULL
                                ORDER BY fx_timestamp_id DESC LIMIT 1""")
        upper_limit = self.cursor.fetchall()[0][0]  # -> Because fetchall outputs sth. like this: [(blabla,)]

        self.cursor.execute(f"""SELECT {pairname}_valid_id FROM fx_data 
                                WHERE fx_timestamp_id BETWEEN {lower_limit} AND {upper_limit}
                                AND {pairname}_valid_id IS NOT NULL 
                                ORDER BY fx_timestamp_id ASC""")

        full_id_batch = np.array(self.cursor.fetchall())[:, 0]
        commit_id_batch = full_id_batch[n_periods-1:] # all IDs minus len of n_periods

        if len(full_id_batch) < n_periods:
            print("Check window size and upper limit!!")

        else:
            from fx_indicators import DatabaseIndicators as di

            if indicator == "up_down":
                indicator_value_batch = di(pairname, n_periods, commit_id_batch).up_down()
            elif indicator == "absolute_body_size":
                indicator_value_batch = di(pairname, n_periods, commit_id_batch).absolute_body_size()
            elif indicator == "cutler_rsi":
                indicator_value_batch = di(pairname, n_periods, commit_id_batch).cutler_rsi()
            else:
                print("Indicator name not defined.")

            for i in range(len(commit_id_batch)):
                self.cursor.execute(f"""UPDATE fx_data SET
                                        {pairname}_{indicator}_{n_periods} = {indicator_value_batch[i]}
                                        WHERE {pairname}_valid_id = {commit_id_batch[i]}""")
                if i % batchsize == 0:
                    self.conn.commit()
            self.conn.commit()
            print("finished.")



if __name__ == "__main__":
    x = InitTools()
    y = IndicatorTools()
    # x.create_initial_table()
    # x.timestamp_fill(start_year=2000, last_year=2002, commit_batch_size=150000)
    # x.create_forex_columns("eurusd")
    # x.update_raw_fx_data(pairname="eurusd", csv_folder="C://Users//Chris//Desktop//ptr-utilities//datasets//eurusd")
    # x.update_valid_id("eurusd")
    # x.fill_empty_fx_rows("eurusd", 100000)

    # y.create_indicator_columns("eurusd")
    # y.fill_indicator_value(indicator="up_down", pairname="eurusd", n_periods=1, batchsize=10000)
    # y.fill_indicator_value(indicator="absolute_body_size", pairname="eurusd", n_periods=1, batchsize=10000)
    # y.fill_indicator_value(indicator="cutler_rsi", pairname="eurusd", n_periods=14, batchsize= 10000)
    # y.fill_indicator_value(indicator="cutler_rsi", pairname="eurusd", n_periods=28, batchsize= 100000)
    # y.fill_indicator_value(indicator="cutler_rsi", pairname="eurusd", n_periods=56, batchsize= 100000)
    # y.fill_indicator_value(indicator="cutler_rsi", pairname="eurusd", n_periods=112, batchsize= 100000)
    # y.fill_indicator_value(indicator="cutler_rsi", pairname="eurusd", n_periods=3000, batchsize= 100000)
