
# bla

class InitTools:
    def __init__(self):
        self.cursor = create_cursor()


    def create_cursor:
        # returns psycopg2 cursor
        return None

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
                months.append(str(i))

            # set list of all possible days (1-31)
            days = []
            for i in range(1, 32):
                days.append(str(i))

            # set list of all hours (0-23)
            hours = []
            for i in range(0, 24):
                hours.append(str(i))

            # set list of all minutes (0-59)
            minutes = []
            for i in range(0, 60):
                minutes.append(str(i))

            try:
                print("start creating timestamps in database...")
                i = 1
                for year in years:
                    for month in months:
                        for day in days:
                            for hour in hours:
                                for minute in minutes:
                                    self.cursor.execute(f"""update ... set ... where ...""")
                                    i += 1
                                    if i % 10000 == 0:
                                        self.cursor.commit()
                                    if i % 100000 == 0:
                                        print(f"{year}/{month}. {i} timestamps added. ")
                self.cursor.commit()
                print(f"finished. {i} timestamps added.")

            except:
                print("error adding timestamps")
