import psycopg2
import csv
import os
import datetime



class InitTools:
    def __init__(self):
        self.conn = self.create_connection()
        self.cursor = self.conn.cursor()

    def create_initial_table(self, indexing=True):
        '''
        reqires set up Database called Forex_Data which was created in PGAdmin using e.g.
        CREATE DATABASE "Forex_Data"
        WITH
        OWNER = postgres
        ENCODING = 'UTF8'
        CONNECTION LIMIT = -1;
        :return: new Tables in Database needed for this project
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
                print("tables set with indexes.")
            else:
                print("tables set.")
        except:
            print("error setting up tables.")


    def create_connection(self):
        return psycopg2.connect(
            host="127.0.0.1",
            dbname="Forex_Data",
            user="postgres",
            password="postgres",
            port="5432")

    def timestamp_fill(self, start_year=2000, last_year=2001, commit_batch_size=50000):
        '''
        makes blablabla
        :param start_year: first years
        :param last_year: last year to be filled
        :return: filled database
        '''

        if start_year > last_year:
            print("error, start year must be smaller or equal to last year")

        else:
            # set list with range of years
            years = []
            for i in range(start_year, last_year+1):
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
                print(f"finished. {i-1} timestamps added. Updating indexes now...")
                self.cursor.execute("REINDEX TABLE fx_data;")
                self.conn.commit()
                print("indexes successfully updated.")
                print("Initial procedure finished.")
            except:
                print("error adding timestamps")

    def create_forex_tables(self, pair_name, indexing=True):
        '''
        Creates the tables required for filling raw data of specified forex pair in database.
        :param pair_name: Name of forex pair. must be string of 6 chars, eg. "eurusd".
        :param indexing: Boolean, if true, index on new columns is added. Default true.
        :return: new tables in database to be filled with raw forex data.
        '''
        print(f"adding Columns for {pair_name}..")
        self.cursor.execute(f"""ALTER TABLE fx_data
                                ADD COLUMN {pair_name}_bidopen double precision,
                                ADD COLUMN {pair_name}_bidhigh double precision,
                                ADD COLUMN {pair_name}_bidlow double precision,
                                ADD COLUMN {pair_name}_bidclose double precision,
                                ADD COLUMN {pair_name}_nonvalid_count integer;""")
        self.conn.commit()
        print("Columns added.")
        if indexing == True:
            print(f"adding Indexes on {pair_name} tables..")
            self.cursor.execute(
                f"""CREATE INDEX idx_{pair_name}_bidopen on fx_data ("{pair_name}_bidopen");
                    CREATE INDEX idx_{pair_name}_bidhigh on fx_data ("{pair_name}_bidhigh");
                    CREATE INDEX idx_{pair_name}_bidlow on fx_data ("{pair_name}_bidlow");
                    CREATE INDEX idx_{pair_name}_bidclose on fx_data ("{pair_name}_bidclose");
                    CREATE INDEX idx_{pair_name}_nonvalid_count on fx_data ("{pair_name}_nonvalid_count");""")
            self.conn.commit()
            print("Indexes added.")



    def insert_raw_fx_data(self, pairname, csv_folder, commit_batch_size=25000):
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
                    self.cursor.execute(f"""UPDATE fx_data SET
                                            {pairname}_bidopen = {line[1]},
                                            {pairname}_bidhigh = {line[2]},
                                            {pairname}_bidlow = {line[3]},
                                            {pairname}_bidclose = {line[4]},
                                            {pairname}_nonvalid_count = 0
                                            WHERE
                                            fx_timestamp = '{line[0]}'""")
                    i += 1
                    if i % commit_batch_size == 0:
                        self.conn.commit()
                    if i % 100000 == 0:
                        print(f"{i} ohlc-lines added..")
                self.conn.commit()
                print(f"file {file} finished.")
        print(f"All files from {filepath} added as {pairname} values.")
        print("reindexing...")
        self.cursor.execute("REINDEX TABLE fx_data")
        print("finished")



if __name__ == "__main__":
    # x = InitTools()
    # x.create_initial_table()
    # x.timestamp_fill(start_year=2000, last_year=2001)
    # x.create_forex_tables("eurusd")
    # x.insert_raw_fx_data(pairname="eurusd", csv_folder="C://Users//Chris//Desktop//ptr-utilities//datasets//eurusd")
    pass
