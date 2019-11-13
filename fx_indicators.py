from database_tools import InitTools
import pandas as pd
import numpy as np
import time


class DatabaseIndicators:
    '''
    Indicators to fill database with. Each Indicator inputs forex pair name, timestamp ID. Outputs Indicator value
    for given timestamp IDs.
    '''
    def __init__(self, pairname, n_periods, valid_ids): # timestamp_ids):
        self.InitToolsObject = InitTools()
        self.conn = self.InitToolsObject.create_connection()
        self.cursor = self.conn.cursor()
        self.pairname = pairname
        self.n_periods = n_periods
        # self.timestamp_ids = timestamp_ids
        self.valid_ids = valid_ids


    def up_down(self):
        '''
        Indicates whether selected candle has an downwards direction (=0, open bid is higher than close bid),
        no direction (=1, open bid and close bid are equal) or an upwards direction (=2, open bid is lower than
        close bid). Needed for eg. RSI determination or other database not yet defined indicators.
        :return: Integer with value 0, 1, or 2.
        '''

        self.cursor.execute(f"""SELECT {self.pairname}_bidopen, {self.pairname}_bidclose FROM fx_data 
                            WHERE {self.pairname}_valid_id BETWEEN {self.valid_ids[0]} AND {self.valid_ids[-1]}
                            ORDER BY fx_timestamp_id ASC""")

        open_close_values = np.array(self.cursor.fetchall())

        output = []
        for i in range(len(self.valid_ids)):
            if float(open_close_values[i][0]) > float(open_close_values[i][1]):
                output.append(0)
            elif float(open_close_values[i][0]) == float(open_close_values[i][1]):
                output.append(1)
            elif float(open_close_values[i][0]) < float(open_close_values[i][1]):
                output.append(2)
        return output


    def absolute_body_size(self):
        try:
            # self.cursor.execute(f"""SELECT {self.pairname}_bidopen, {self.pairname}_bidclose FROM fx_data
            #                     WHERE fx_timestamp_id BETWEEN {self.timestamp_ids[0]} AND {self.timestamp_ids[-1]}
            #                     ORDER BY fx_timestamp_id ASC""")

            self.cursor.execute(f"""SELECT {self.pairname}_bidopen, {self.pairname}_bidclose FROM fx_data 
                                WHERE {self.pairname}_valid_id BETWEEN {self.valid_ids[0]} AND {self.valid_ids[-1]}
                                ORDER BY fx_timestamp_id ASC""")

            open_close_values = self.cursor.fetchall()

            output = []
            for i in range(len(self.valid_ids)):
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
                            WHERE {self.pairname}_valid_id BETWEEN {self.valid_ids[0] - self.n_periods}
                            AND {self.valid_ids[-1]}
                            ORDER BY fx_timestamp_id ASC""")
        fetch = np.array(self.cursor.fetchall())
        output = []
        for i in range(len(fetch) - (self.n_periods - 1)):
            ups = 0
            downs = 0
            for j in range(self.n_periods):
                if fetch[i+j][0] == 2:
                    ups += float(fetch[i+j][1])
                elif fetch[i+j][0] == 0:
                    downs += float(fetch[i+j][1])
            if downs == 0:
                downs += 0.0000001
            ups /= self.n_periods
            downs /= self.n_periods
            rs = ups/downs
            rsi = 100-(100 / (1 + rs))
            output.append(float(np.around(rsi, decimals=3)))

        return output

    def dmi(self):
        if self.valid_ids[0] == 0:
            self.cursor.execute(f"""SELECT {self.pairname}_bidhigh, 
                                           {self.pairname}_bidlow, 
                                           {self.pairname}_bidclose FROM fx_data
                                WHERE {self.pairname}_valid_id BETWEEN {self.valid_ids[0] - self.n_periods}
                                AND {self.valid_ids[-1]}
                                ORDER BY fx_timestamp_id ASC""")
            fetch = np.array(self.cursor.fetchall(), dtype=np.float)
            output = [0.0]
        elif self.valid_ids[0] >= 0:
            self.cursor.execute(f"""SELECT {self.pairname}_bidhigh, 
                                           {self.pairname}_bidlow, 
                                           {self.pairname}_bidclose FROM fx_data
                                WHERE {self.pairname}_valid_id BETWEEN {self.valid_ids[0] - 1 - self.n_periods}
                                AND {self.valid_ids[-1]}
                                ORDER BY fx_timestamp_id ASC""")
            fetch = np.array(self.cursor.fetchall(), dtype=np.float)
            output = []

        for i in range(1, len(fetch)-1):
            pdm = fetch[i][0] - fetch[i-1][0] if fetch[i][0] - fetch[i-1][0] > 0 else 0
            ndm = fetch[i][1] - fetch[i-1][1] if fetch[i][1] - fetch[i-1][1] > 0 else 0
            tr = max(
                abs(fetch[i][0] - fetch[i][1]),
                abs(fetch[i-1][2] - fetch[i][0]),
                abs(fetch[i-1][2] - fetch[i][1])
                    )
            if tr == 0:
                dmi = 0
            else:
                pdi = pdm / tr
                ndi = ndm / tr
                if pdi + ndi == 0:
                    dmi = 0
                else:
                    dmi = abs((pdi - ndi) / (pdi + ndi))
            output.append(np.float(np.around(dmi, 5)))
        return np.array(output, dtype=np.float)



    def sma_oc(self):

        self.cursor.execute(f"""SELECT {self.pairname}_bidopen, {self.pairname}_bidclose FROM fx_data
                            WHERE {self.pairname}_valid_id BETWEEN {self.valid_ids[0] - self.n_periods}
                            AND {self.valid_ids[-1]}
                            ORDER BY fx_timestamp_id ASC""")
        fetch = np.array(self.cursor.fetchall(), dtype=np.float)
        output = []
        for i in range(len(fetch) - (self.n_periods - 1)):
            output.append(np.around(fetch[i:i + self.n_periods].mean(), 5))
        return output

    def adx(self):
        self.cursor.execute(f"""SELECT {self.pairname}_dmi_1 FROM fx_data
                            WHERE {self.pairname}_valid_id BETWEEN {self.valid_ids[0] - self.n_periods}
                            AND {self.valid_ids[-1]}
                            ORDER BY fx_timestamp_id ASC""")
        fetch = np.array(self.cursor.fetchall(), dtype=np.float)
        output = np.empty(len(fetch) - (self.n_periods - 1))
        for i in range(len(fetch) - (self.n_periods - 1)):
            output[i] = fetch[i:i + self.n_periods].mean()
        return np.around(output, 3)



    def psar(self):
        pass






class LiveIndicators:
    '''
    Indicators to be used in live mode. Each Indicator inputs generic OHLC-Array of lengh n. Outputs Indicator value
    for element n of Array. Can be used with database, too. For filling database with Indicator values, utilize
    Indicators from DatabaseIndicators (better speed / performance).
    '''
    def __init__(self, ohlc_batch):
        self.ohlc_batch = ohlc_batch

    def up_down(self):
        output = []
        for candle in self.ohlc_batch:
            if candle[0] > candle[3]:
                output.append(0)
            elif candle[0] == candle[3]:
                output.append(1)
            elif candle[0] < candle[3]:
                output.append(2)
        return output

    def absoulte_body_size(self):
        output = []
        for candle in self.ohlc_batch:
            output.append(np.around(abs(candle[0] - candle[1]), decimals=5))
        return output

    def cutler_rsi(self, n_periods):
        output = [x*0 for x in range(n_periods - 1)]
        current_candle_pos = len(self.ohlc_batch) - 1 # because len gives 'whole' len which is 1 too much
        while current_candle_pos >= n_periods - 1:
            ups = 0
            downs = 0
            current_candle = self.ohlc_batch[current_candle_pos]
            for i in range(n_periods):
                j = current_candle_pos - i
                if self.ohlc_batch[j][0] > self.ohlc_batch[j][3]:
                    downs += self.ohlc_batch[j][0] - self.ohlc_batch[j][3]
                elif self.ohlc_batch[j][0] < self.ohlc_batch[j][3]:
                    ups += self.ohlc_batch[j][3] - self.ohlc_batch[j][0]
            if downs == 0:
                rsi = 100
            else:
                ups /= n_periods
                downs /= n_periods
                rs = ups / downs
                rsi = 100-(100 / (1 + rs))
            output.append(float(np.around(rsi, decimals=3)))
            current_candle_pos -= 1
        return output






if __name__ == "__main__":
    pass
