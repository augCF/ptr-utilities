from database_tools import InitTools
import pandas as pd
import numpy as np
import time


class DatabaseIndicators:
    '''
    Indicators to fill database with. Each Indicator inputs forex pair name, timestamp ID. Outputs Indicator value
    for given timestamp IDs.
    '''
    def __init__(self, pairname, n_periods, timestamp_ids):
        self.InitToolsObject = InitTools()
        self.conn = self.InitToolsObject.create_connection()
        self.cursor = self.conn.cursor()
        self.pairname = pairname
        self.n_periods = n_periods
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
                                WHERE fx_timestamp_id BETWEEN {self.timestamp_ids[0]} AND {self.timestamp_ids[-1]}
                                ORDER BY fx_timestamp_id ASC""")
            open_close_values = self.cursor.fetchall()

            output = []
            for i in range(len(self.timestamp_ids)):
                if float(open_close_values[i][0]) > float(open_close_values[i][1]):
                    output.append(0)
                elif float(open_close_values[i][0]) == float(open_close_values[i][1]):
                    output.append(1)
                elif float(open_close_values[i][0]) < float(open_close_values[i][1]):
                    output.append(2)
            return output

        except:
            pass

    def absolute_body_size(self):
        try:
            self.cursor.execute(f"""SELECT {self.pairname}_bidopen, {self.pairname}_bidclose FROM fx_data 
                                WHERE fx_timestamp_id BETWEEN {self.timestamp_ids[0]} AND {self.timestamp_ids[-1]}
                                ORDER BY fx_timestamp_id ASC""")
            open_close_values = self.cursor.fetchall()

            output = []
            for i in range(len(self.timestamp_ids)):
                output.append(np.around(abs(float(open_close_values[i][0]) - float(open_close_values[i][1])), decimals=5))
            return output

        except:
            print("Error at absolute body size")


    def cutler_rsi(self):
        '''
        Variation of Welles Wilders RSI. Doesnt consider previous RSI Values (no smoothing).
        :return:
        '''

        self.cursor.execute(f"""SELECT {self.pairname}_up_down_1, {self.pairname}_absolute_body_size_1 FROM fx_data
                            WHERE fx_timestamp_id BETWEEN {self.timestamp_ids[0] - (self.n_periods - 1)}
                            AND {self.timestamp_ids[-1]}
                            ORDER BY fx_timestamp_id ASC""")
        fetch = self.cursor.fetchall()
        output = []
        for i in range(len(self.timestamp_ids) - 1):
            ups = 0
            downs = 0
            for j in range(self.n_periods):
                if fetch[i+j][0] == 2:
                    ups += float(fetch[i+j][1])
                elif fetch[i+j][0] == 0:
                    downs += float(fetch[i+j][1])
                else:
                    ups += 0.0000001
                    downs += 0.0000001
            ups /= self.n_periods
            downs /= self.n_periods
            rs = ups/downs
            rsi = 100-(100 / (1 + rs))
            output.append(float(np.around(rsi, decimals=3)))

        return output









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
