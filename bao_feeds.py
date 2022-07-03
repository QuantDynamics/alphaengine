import backtrader as bt


class BaoStockCSVData(bt.feeds.GenericCSVData):
    """
    数据说明：
    dataetime: 交易时间
    code: 股票代码
    open: 开盘价，小数点后四位，人民币
    high: 最高价，小数点后四位，人民币
    low: 最低价，小数点后四位，人民币
    close: 收盘价，小数点后四位，人民币
    preclose: 昨日收盘价，
            证券在指定交易日行情数据的前收盘价，当日发生除权除息时，“前收盘价”不是前一天的实际收盘价，
            而是根据股权登记日收盘价与分红现金的数量、配送股的数里和配股价的高低等结合起来算出来的价格。
            1、计算除息价:
                除息价=股息登记日的收盘价-每股所分红利现金额
            2、计算除权价:
                送红股后的除权价=股权登记日的收盘价/(1+每股送红股数)
                配股后的除权价=(股权登记日的收盘价+配股价*每股配股数)/(1+每股配股数)
            3、计算除权除息价
                除权除息价=(股权登记日的收盘价-每股所分红利现金额+配股价*每股配股数)/(1+每股送红股数+每股配股数)
            “前收盘价”由交易所计算并公布。首发日的“前收盘价”等于“首发价格”。
    volume: 成交数量，单位股数
    amount: 成交金额，小数点后四位，人民币
    adjustflag: 复权状态，前复权，后复权
    turn: 换手率，小数点后六位，%
    tradestatus: 交易状态，1:正常交易；2:停牌
    pctChg: 涨跌幅，小数点后六位
    isST: 是否ST
    """
    lines = ('preclose', "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST")
    params = (
        ('nullvalue', float('NaN')),
        ('dtformat', '%Y-%m-%d'),
        ('tmformat', '%H:%M:%S'),
        ('datetime', 0),
        ('time', -1),
        ('code', 1),
        ('open', 2),
        ('high', 3),
        ('low', 4),
        ('close', 5),
        ('preclose', 6),
        ('volume', 7),
        ('amount', 8),
        ('adjustflag', 9),
        ('turn', 10),
        ('tradestatus', 11),
        ('pctChg', 12),
        ('isST', 13),
    )
