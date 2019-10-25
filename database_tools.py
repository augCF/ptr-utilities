import psycopg2
# bla

class InitTools:
    def __init__(self):
        self.conn = self.create_connection()
        self.cursor = self.conn.cursor()

    def create_initial_tables(self):
        '''
        reqires set up Database called Forex_Data which was created in PGAdmin using e.g.
        CREATE DATABASE "Forex_Data"
        WITH
        OWNER = postgres
        ENCODING = 'UTF8'
        CONNECTION LIMIT = -1;
        :return: new Tables in Database needed for this project
        '''
        self.cursor.execute("""CREATE TABLE public.fx_data
                                (
                                    fx_timestamp_id integer,
                                    fx_timestamp character varying(15)
                                )
                                WITH (
                                    OIDS = FALSE
                                );
                                
                                ALTER TABLE public.fx_data
                                    OWNER to postgres;
                                ALTER TABLE public.fx_data
                                    ADD PRIMARY KEY (fx_timestamp_id)
                            ;""")
        self.conn.commit()


    def create_connection(self):
        return psycopg2.connect(
            host="127.0.0.1",
            dbname="Forex_Data",
            user="postgres",
            password="postgres",
            port="5432")


    def timestamp_fill(self, start_year=2000, last_year=2001):
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
                            for hour in hours:
                                for minute in minutes:
                                    self.cursor.execute(f"""INSERT INTO fx_data
                                                        (fx_timestamp_id, fx_timestamp)
                                                        VALUES
                                                        ({i}, '{year}{month}{day} {hour}{minute}00')""")
                                    i += 1
                                    if i % 50000 == 0:
                                        self.conn.commit()
                                    if i % 100000 == 0:
                                        print(f"{year}/{month}. {i} timestamps added. ")
                self.conn.commit()
                print(f"finished. {i-1} timestamps added. creating index now...")
                self.cursor.execute("CREATE UNIQUE INDEX idx_fx_timestamp_id ON fx_data (fx_timestamp_id)")
                self.cursor.execute("CREATE UNIQUE INDEX idx_fx_timestamp ON fx_data (fx_timestamp)")
                self.conn.commit()
                print("Indexes finished.")
            except:
                print("error adding timestamps")


if __name__ == "__main__":
    x = InitTools()
    x.timestamp_fill(start_year=2000, last_year=2000)