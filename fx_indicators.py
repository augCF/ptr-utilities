from database_tools import InitTools


class DatabaseIndicators:
    '''
    Indicators to fill database with. Each Indicator inputs forex pair name, timestamp ID. Outputs Indicator value
    for given timestamp ID.
    '''
    def __init__(self):
        self.InitToolsObject = InitTools()
        self.conn = self.InitToolsObject.create_connection()
        self.cursor = self.conn.cursor()

    def up_down_candle(self, pairname, timestamp_id):
        '''
        Indicates whether selected candle has an downwards direction (=0, open bid is higher than close bid),
        no direction (=1, open bid and close bid are equal) or an upwards direction (=2, open bid is lower than
        close bid). Needed for eg. RSI determination or other database indicators.
        :param pairname: Name of forex pair. must be string of 6 chars, eg. "eurusd".
        :param timestamp_id: Valid ID within fx_timestamp_id.
        :return: Integer with value 0, 1, or 2.
        '''

        self.cursor.execute(f"""SELECT {pairname}_bidopen, {pairname}_bidclose FROM fx_data 
                                where fx_timestamp_id = {timestamp_id}""")
        try:
            open_close_values = self.cursor.fetchall()[0]
            if open_close_values[0] > open_close_values[1]:
                return 0
            elif open_close_values[0] == open_close_values[1]:
                return 1
            elif open_close_values[0] < open_close_values[1]:
                return 2
            else:
                print(f"up / down values couldnt be determined for {pairname} at {timestamp_id}")
        except:
            print(f"up / down values couldnt be determined for {pairname} at {timestamp_id}")



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
