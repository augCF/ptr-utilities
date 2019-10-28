from database_tools import InitTools


class DatabaseIndicators:
    '''
    Indicators to fill database with. Each Indicator inputs forex pair name, timestamp ID. Outputs Indicator value
    for given timestamp ID.
    '''
    def __init__(self, pairname, windowsize, timestamp_ids):
        self.InitToolsObject = InitTools()
        self.conn = self.InitToolsObject.create_connection()
        self.cursor = self.conn.cursor()
        self.pairname = pairname
        self.windowsize = windowsize
        self.timestamp_ids = timestamp_ids


    def up_down(self):
        '''
        Indicates whether selected candle has an downwards direction (=0, open bid is higher than close bid),
        no direction (=1, open bid and close bid are equal) or an upwards direction (=2, open bid is lower than
        close bid). Needed for eg. RSI determination or other database not yet defined indicators.
        :return: Integer with value 0, 1, or 2.
        '''

        try:
            self.cursor.execute(f"""SELECT {self.pairname}_bidopen, {self.pairname}_bidclose FROM fx_data 
                                WHERE fx_timestamp_id BETWEEN {self.timestamp_ids[0]} AND {self.timestamp_ids[-1]}""")
            open_close_values = self.cursor.fetchall()

            output = []
            for i in range(len(self.timestamp_ids)):
                if open_close_values[i][0] > open_close_values[i][1]:
                    output.append(0)
                elif open_close_values[i][0] == open_close_values[i][1]:
                    output.append(1)
                elif open_close_values[i][0] < open_close_values[i][1]:
                    output.append(2)
            return output

        except:
            pass

    def rsi(self):
        '''
        Welles Wilders
        :return:
        '''




class LiveIndicators:
    '''
    Indicators to be used in live mode. Each Indicator inputs generic OHLC-Array of lengh n. Outputs Indicator value
    for element n of Array. Can be used with database, too. For filling database with Indicator values, utilize
    Indicators from DatabaseIndicators (better speed / performance).
    '''
    def __init__(self):
        pass


if __name__ == "__main__":
    pass
