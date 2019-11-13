import fx_indicators


class Teststrategy:
    def __init__(self):
            self.x_width = 80

    def decide(self, minute_bar_array, equity, balance):
        rsi_30 = fx_indicators.LiveIndicators(minute_bar_array).cutler_rsi(70)
        if sum(rsi_30)/(self.x_width - 70) > 70:
            x = "s"
        # elif sum(rsi_30)/(self.x_width - 30) < 10:
        #     x = "b"
        # elif equity > balance + 10:
        #     x = "c"
        else:
            x = "n"

        return x
