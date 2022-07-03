import backtrader as bt


class SignalThreeNeg(bt.Indicator):
    """
    连续三个大阴线信号，连续三天大阴超过 neg_ratio
    """
    lines = ("three_neg", )
    params = (
        ("neg_ratio", 7),
        ("neg_days", 3),
    )
    plotinfo = dict(plot=True, subplot=True, plotname="three_neg")

    def __init__(self):
        # 连续大阴的周期数
        self.ult_neg_num = 0
        pass

    def next(self):
        if self.data.close[0] > self.data.open[0]:
            self.lines.three_neg[0] = 0
            self.ult_neg_num = 0
        else:
            hl_diff = self.data.high[0] - self.data.low[0] / self.data.open[0]
            if hl_diff > self.p.neg_ratio:
                self.ult_neg_num += 1
            else:
                self.ult_neg_num = 0
        if self.ult_neg_num == self.p.neg_days:
            self.lines.three_neg[0] = 1
        else:
            self.lines.three_neg[0] = 0

    def stop(self):
        self.ult_neg_num = 0
