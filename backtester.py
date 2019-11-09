'''
Description follows..
'''

import numpy as np
from database_tools import InitTools
from trading_strategies import Teststrategy



class Backtest:

    def __init__(self, max_open_orders):
        self.number_of_open_orders = 0
        self.max_open_orders = max_open_orders
        self.orderlist = np.zeros((max_open_orders, 7))  # type, price, tp, sl, value, swap
        self.pairname = "eurusd"
        self.spread = 15  # points
        self.pipsize = 1 / 10000
        self.balance = 0
        self.equity = 0
        self.free_margin = 0
        self.leverage = 30
        self.ohlc = np.array([0, 0, 0, 0], dtype=np.float)
        self.next_ohlc = np.array([0, 0, 0, 0], dtype=np.float)
        self.conn = InitTools().create_connection()
        self.cursor = self.conn.cursor()

    def update_ohlcs(self, ohlc, next_ohlc):
        self.ohlc = np.array(ohlc)
        self.next_ohlc = np.array(next_ohlc)

    def open_order(self, ordertype, lot, tp_points, sl_points):
        if self.number_of_open_orders < self.max_open_orders:
            # market_price = self.next_ohlc.sum()/4
            market_price = np.around(np.random.uniform(self.next_ohlc[2], self.next_ohlc[1]), 5)
            one_pip = market_price * self.pipsize
            if ordertype == "b":
                open_price = np.around(market_price - ((self.spread / 2) * one_pip), 5)
                tp = np.around(open_price + (tp_points * one_pip), 5)
                sl = np.around(open_price - (sl_points * one_pip), 5)
            elif ordertype == "s":
                open_price = np.around(market_price + ((self.spread / 2) * one_pip), 5)
                tp = np.around(open_price - (tp_points * one_pip), 5)
                sl = np.around(open_price + (sl_points * one_pip), 5)
            self.orderlist[self.number_of_open_orders][0] = 1 if ordertype == "b" else 2
            self.orderlist[self.number_of_open_orders][1] = lot
            self.orderlist[self.number_of_open_orders][2] = open_price
            self.orderlist[self.number_of_open_orders][3] = tp
            self.orderlist[self.number_of_open_orders][4] = sl
            self.orderlist[self.number_of_open_orders][5] = np.around(
                -abs((market_price - open_price) * lot * 1 / self.pipsize), 2)  # value
            self.orderlist[self.number_of_open_orders][6] = 0  # swap
            self.number_of_open_orders += 1
        else:
            pass

    def update_order_list(self):
        # first, update all order values and sort the list (best = highest)
        # second, check if orders shall be closed because of tp or sl
        market_price = self.ohlc.sum() / 4
        orders_to_be_closed = ()
        for i in range(self.max_open_orders):
            if self.orderlist[i][0] == 1:  # buy order
                self.orderlist[i][5] = np.around(
                    (market_price - self.orderlist[i][2]) * self.orderlist[i][1] * 1 / self.pipsize, 2)
                if self.ohlc[1] > self.orderlist[i][3]:  # if current high is higher than take profit
                    orders_to_be_closed += (i,)
                elif self.ohlc[2] <= self.orderlist[i][4]:  # if current low is lower than stop loss
                    orders_to_be_closed += (i,)
            elif self.orderlist[i][0] == 2:  # sell order
                self.orderlist[i][5] = np.around(
                    (self.orderlist[i][2] - market_price) * self.orderlist[i][1] * 1 / self.pipsize, 2)
                if self.ohlc[2] < self.orderlist[i][3]:  # if current low is lower than take profit
                    orders_to_be_closed += (i,)
                elif self.ohlc[1] >= self.orderlist[i][4]:  # if current high is higher than stop loss
                    orders_to_be_closed += (i,)

        # third, close all identified tp/sl orders
        if len(orders_to_be_closed) > 0:
            for order in orders_to_be_closed:
                self.close_order(order)
            self.update_order_list()

        # fourth, sort new orderlist along value desc
        self.orderlist = -self.orderlist
        self.orderlist = -self.orderlist[self.orderlist[:, 5].argsort()]  # Sorts the Array along Cell 5 (value)
        self.orderlist = -self.orderlist
        self.orderlist = -self.orderlist[self.orderlist[:, 1].argsort()]  # Sorts the Array alon Cell 1 (lot)

        # fifth, update margin!! dont know the right formula yet. Will do that another time

    def update_swaps(self):
        for i in range(self.max_open_orders):
            if self.orderlist[i][0] == 1:
                self.orderlist[i][6] += 0.00
            elif self.orderlist[i][0] == 2:
                self.orderlist[i][6] -= 0.10
        # SWAPS MUST BE CORRECTLY CALCULATED. THIS IS JUST A PLACEHOLDER.

    def close_order(self, order_number):
        close_price = np.around(np.random.uniform(self.next_ohlc[1], self.next_ohlc[2]), 5)
        if self.orderlist[order_number][0] == 1:  # "b"
            close_value = np.around((self.orderlist[order_number][2] - close_price) * \
                                    self.orderlist[order_number][1] * 1 / self.pipsize, 2)
        elif self.orderlist[order_number][0] == 2:
            close_value = np.around((close_price - self.orderlist[order_number][2]) * \
                                    self.orderlist[order_number][1] * 1 / self.pipsize, 2)
        self.balance += close_value
        self.orderlist[order_number] = np.zeros(7)
        self.number_of_open_orders -= 1


    def RunBacktest(self, start_id, end_id, strategy):
        self.cursor.execute(f"""SELECT   {self.pairname}_bidopen,
                                        {self.pairname}_bidhigh,
                                        {self.pairname}_bidlow,
                                        {self.pairname}_bidclose,
                                        fx_hour,
                                        fx_weekday
                                FROM fx_data WHERE {self.pairname}_valid_id BETWEEN
                                {start_id - strategy.x_width + 1} AND {end_id}
                                ORDER BY fx_timestamp_id ASC""")
        full_fetch = np.array(self.cursor.fetchall(), dtype=np.float)

        finished = False
        ff_id = strategy.x_width - 1  # is the translated start id
        hour = full_fetch[ff_id][4]
        while finished == False:
            if ff_id >= len(full_fetch) - 2:  # 2 because of next_ohlc!
                finished = True

            if full_fetch[ff_id][4] == 17 and hour == 16:  # update swaps daily
                self.update_swaps()
                if full_fetch[ff_id][5] == 3:  # before weekend update 3 times
                    self.update_swaps()
                    self.update_swaps()
            hour = full_fetch[ff_id][4]

            self.update_order_list()

            self.update_ohlcs(full_fetch[ff_id, :4], full_fetch[ff_id + 1, :4])

            minute_bar_array_to_evaluate = full_fetch[ff_id - (strategy.x_width - 1):ff_id, :4]
            decision = strategy.decide(minute_bar_array_to_evaluate, self.equity, self.balance)

            if decision == "b":
                self.open_order("b", 0.01, 100, 100)
            if decision == "s":
                self.open_order("s", 0.01, 100, 100)
            if decision == "n":
                pass
            if decision == "c":
                for i in range(self.max_open_orders):
                    self.close_order(i)

            ff_id += 1
            if ff_id % 5000 == 0:
                print(f"balance: {np.around(self.balance, 2)}")


if __name__ == "__main__":
    x = Backtest(5)
    x.equity = 1000
    x.balance = 1000
    strategy = Teststrategy()
    print(strategy.x_width)
    x.RunBacktest(4000000, 5000000, strategy)
