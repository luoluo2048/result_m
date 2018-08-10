#!/usr/bin/env python
# -*- coding: utf-8 -*-

import msvcrt
import sys
import TradeX2 as TradeX
import os
import time
import codecs
import numpy as np
import pandas as pd
from pandas import DataFrame, read_csv

# 此处读取account.xlsx文件里面的账户信息。
Account = pd.read_excel("account.xlsx")
account_num = len(list(Account.nQsid))
HostHq = pd.read_excel("HostHq.xlsx")
HostHq_num = len(list(HostHq.port))

for inx in range(0, account_num):
    Account.iat[inx, 7] = Account.sAccountNo[inx].split('.')[0]  # 把.S的后缀去掉.
    Account.iat[inx, 9] = Account.sTradeAccountNosz[inx].split('.')[0]  # 把.S的后缀去掉.
    Account.iat[inx, 10] = Account.sPassword[inx].split('.')[0]  # 把.S的后缀去掉.
nQsids = list(Account.nQsid)
sHosts = list(Account.sHost)
nPorts = list(Account.nPort)
sVersions = list(Account.sVersion)
nBranchIDs = list(Account.nBranchID)
nAccountTypes = list(Account.nAccountType)
sAccountNos = list(Account.sAccountNo)
sTradeAccountNoshs = list(Account.sTradeAccountNosh)
sTradeAccountNoszs = list(Account.sTradeAccountNosz)
sPasswords = list(Account.sPassword)

f = open("result_log.txt", 'a')
print >> f, "\nWelcome to the trading system of Heisen Asset Management!"
print "\nWelcome to the trading system of Heisen Asset Management!"
f.close()

Weituo_here = pd.read_excel("./result_weituo.xlsx")
for inx in range(0, len(Weituo_here.stockname)):
    Weituo_here.iat[inx, 0] = Weituo_here.stockname[inx].split('.')[0]  # 把.SZ和.SH的字样去掉.
	# Weituo_here.iat[inx, 0] = Weituo_here.stockname[inx][0:6]
    Weituo_here.iat[inx, 6] = Weituo_here.account[inx].split('.')[0]  # 把account的.S字样去掉.
Weituo_ins = pd.DataFrame({'stockname': ['000001.SZ'], 'bianhao': ['100'], 'amount': [0], 'money': [0.0], 'price': [0.0], 'buysell': [0], 'account': ['1092.S'], 'time': ['11:08']})
Weituo_ins = Weituo_ins[['stockname', 'bianhao', 'amount', 'money', 'price', 'buysell', 'account', 'time']]

timeindex = 1  # 记录while 1这个循环的循环次数
isok = 0  # 记录是否完成调仓任务, 0表示任务未完成， 1表示任务完成。
while 1:
    iftrade = 0  # 标记当时是否可交易， 1表示yes， 0表示no
    nowtime = time.localtime()  # 判断是否是交易时间
    if nowtime[3] == 9 and nowtime[4] > 31:
        iftrade = 1
    elif nowtime[3] in [10, 13, 14]:
        iftrade = 1
    elif nowtime[3] == 11 and nowtime[4] < 30:
        iftrade = 1
    elif nowtime[3] == 14 and nowtime[4] > 57:
        iftrade = 0
    if iftrade == 0:
        print "Non Trading time, waiting ..."
        print time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        time.sleep(30)
        continue  # 如果非交易时间测试下单程序，需要#掉这个continue语句。

    Stock_buy = read_csv("./result_send.csv", dtype={'stockname': str})
    # 从csv文件读取建仓目标
    print "To read result_send.csv:  "
    print time.strftime("%Y%m%d %H:%M:%S", time.localtime())
    print Stock_buy.drop('name', axis=1)
    sleeptime = Stock_buy.ix[0, 'sleeptime']
    # 只读取调用了第一行数据的“sleeptime”数值。sleeptime是对股票账户循环过程中的等待时间。
    # 目前是“股票循环”内嵌于“账号循环”，暂无法对不同的股票设置不同的sleeptime.
    isok = 1  # 如果下面正常运行，未触发更改isok，则isok = 1

    for inx in range(0, len(Stock_buy.stockname)):
        Stock_buy.iat[inx, 0] = Stock_buy.stockname[inx].split('.')[0]  # 把.SZ和.SH的字样去掉.

    total_rows = len(Stock_buy)
    #  用作标记Stock_buy的有效数据行数

    f = open("result_log.txt", 'a')
    print >> f, time.strftime("%Y%m%d %H:%M:%S", time.localtime())
    print >> f, '-------------------------------------------'
    print >> f, 'Portfolio wanted to be: '
    print >> f, Stock_buy
    print >> f, '-------------------------------------------'

    print >> f, "1、初始化TDX...\n"
    TradeX.OpenTdx(14, "6.40", 12, 0)

    index_host = 0
    while index_host < HostHq_num:
        sHost = HostHq.iloc[index_host, 0]
        nPort = int(HostHq.iloc[index_host, 1])
        try:
            clientHq = TradeX.TdxHq_Connect(sHost, nPort)
        except TradeX.TdxHq_error, e:
            print >> f, index_host
            print >> f, "switch to next HostHq"
            index_host = index_host + 1
            continue
        break

    StockValue_all = DataFrame(np.zeros((1, 5)), index=['a'], columns=['stocknum', 'ratio', 'asset', 'amount', 'account'])
    # StockValue_all用于存储各个账号的持仓

    index = 0
    Sendamount_all = 0
    while index < account_num:
        nQsid = int(nQsids[index])
        sHost = str(sHosts[index])
        nPort = int(nPorts[index])
        sVersion = str(sVersions[index])
        nBranchID = int(nBranchIDs[index])
        nAccountType = int(nAccountTypes[index])
        sAccountNo = str(sAccountNos[index])
        sTradeAccountNo = str(sTradeAccountNoshs[index])
        if nQsid in [32, 28]:
            sTradeAccountNo = sAccountNo
        sTradeAccountNosz = str(sTradeAccountNoszs[index])
        sPassword = str(sPasswords[index])
        sTxPassword = ""

        print >> f, '-------------------------------------------'
        print >> f, "登录交易账户:  " + sAccountNo
        print "登录交易账户:  " + sAccountNo

        try:
            client = TradeX.Logon(nQsid, sHost, nPort, sVersion, nBranchID,
                                  nAccountType, sAccountNo, sTradeAccountNo,
                                  sPassword, sTxPassword)
        except TradeX.error, e:
            print >> f, "error_a: "
            print e.message
            print ("nQsid: {}  nHost:{}   nPort: {}  sVersion:{} "
                   "nBranchID:{} nAccountType:{} sAcccountNo:{} sTradeAccountNo:{}"
                   .format(nQsid, sHost, nPort, sVersion, nBranchID, nAccountType, sAccountNo, sTradeAccountNo))
            TradeX.CloseTdx()
            sys.exit(-1)

        nCategory = 0  # 查询资金
        status, content = client.QueryData(nCategory)
        if status < 0:
            print >> f, "error_b: " + str(content)
            print "error_b" + content.decode('GBK')
        else:
            print content.decode('GBK')
            temp1 = content.split('\n')
            temp2 = temp1[1].split('\t')
            if nQsid == 36:  # 中泰，查询到的是总资产
                TotalAsset = float(temp2[7])
                Available_money = float(temp2[3])
            if nQsid == 43:  # 华林
                # print content.decode('GBK')
                TotalAsset = float(temp2[5])  # 对于华林，查询到的是总现金资产。
                Available_money = float(temp2[2])
            if nQsid == 16:
                Available_money = float(temp2[2])  # 对于招商，查询到的是总现金资产
                TotalAsset = float(temp2[2])
            if nQsid == 32:
                Available_money = float(temp2[3])
                TotalAsset = float(temp2[7])  # 对于中信，查询到的是总资产
            if nQsid == 28:  # 光大
                Available_money = float(temp2[2])
                TotalAsset = float(temp2[4])  # 对于光大，查询到的是总资产

        Category = 1  # 查询股份
        status, content = client.QueryData(Category)
        if status < 0:
            print >> f, "error_c: "
        else:
            # print content.decode('GBK')
            temp1 = content.split('\n')
            len12 = len(temp1)
            StockValue = DataFrame(np.zeros((len12 - 1, 5)),
                                   columns=['stocknum', 'ratio', 'asset', 'amount', 'account'])
            for i in range(1, len12):
                temp2 = temp1[i].split('\t')
                if nQsid == 32:  # 中信
                    StockValue.iloc[i - 1, 2] = float(temp2[7])  # 最新市值
                    StockValue.iloc[i - 1, 3] = int(float(temp2[3]))  # 可卖证券数，需要根据不同nQsid调整。
                    StockValue.iloc[i - 1, 0] = temp2[0]  # temp2的第1个数即脚标0，对应着股票代码
                    StockValue.iloc[i - 1, 4] = sAccountNo  # 资金账号
                elif nQsid == 36:  # 中泰
                    StockValue.iloc[i - 1, 2] = float(temp2[9])  # 市值
                    StockValue.iloc[i - 1, 3] = int(temp2[4])  # 可卖证券数，需要根据不同nQsid调整。
                    StockValue.iloc[i - 1, 0] = temp2[0]  # temp2的第1个数即脚标0，对应着股票代码
                    StockValue.iloc[i - 1, 4] = sAccountNo  # 资金账号
                elif nQsid == 43:  # 华林
                    # StockValue.iloc[i - 1, 2] = float(temp2[8])  # 最新市值 20180723数据报错修改这一行
                    StockValue.iloc[i - 1, 2] = float(temp2[7])
                    StockValue.iloc[i - 1, 3] = int(temp2[4])  # 可卖证券数，需要根据不同nQsid调整。
                    StockValue.iloc[i - 1, 0] = temp2[0]  # temp2的第1个数即脚标0，对应着股票代码
                    StockValue.iloc[i - 1, 4] = sAccountNo  # 资金账号
                elif nQsid == 16:  # 招商
                    StockValue.iloc[i - 1, 2] = float(temp2[8])  # 最新市值
                    StockValue.iloc[i - 1, 3] = int(temp2[2])  # 可卖证券数，需要根据不同nQsid调整。
                    StockValue.iloc[i - 1, 0] = temp2[12]  # 股票代码
                    StockValue.iloc[i - 1, 4] = sAccountNo  # 资金账号
                elif nQsid == 28:  # 光大
                    StockValue.iloc[i - 1, 2] = float(temp2[6])  # 最新市值
                    StockValue.iloc[i - 1, 3] = int(float(temp2[2]))  # 可卖证券数，需要根据不同nQsid调整。
                    StockValue.iloc[i - 1, 0] = temp2[8]  # 股票代码
                    StockValue.iloc[i - 1, 4] = sAccountNo  # 资金账号

            if nQsid in [43, 16]:
                TotalAsset = TotalAsset + StockValue.asset.sum()
                # 华林、招商的TotalAsset需要分别查询资金、股票再求和。

            for i in range(1, len12):
                StockValue.iloc[i - 1, 1] = float(StockValue.iloc[i - 1, 2]) / TotalAsset * 100.0  # 仓位比例

            StockValue_all = pd.concat([StockValue_all, StockValue], axis=0)
            print >> f, "Current Portfolio of the Account: " + str(sAccountNo)
            print "Current Portfolio of the Account: " + sAccountNo
            print >> f, str(StockValue)

        Category = 4  # 查询可撤单
        status, content = client.QueryData(Category)
        if status < 0:
            print >> f, "error_c: "
        else:
            # print content.decode('GBK')
            temp1 = content.split('\n')
            len12 = len(temp1)
            for i in range(1, len12):
                temp2 = temp1[i].split('\t')  # temp2为查询到的某一行
                if nQsid in [36, 43, 28]:  # 中泰、华林、光大
                    market = int(temp2[13])  # market
                    order_index = temp2[9]  # 委托编号
                if nQsid == 16:
                    market = int(temp2[12])
                    order_index = temp2[9]
                if nQsid == 32:
                    market = int(temp2[16])
                    order_index = temp2[1]
                status, content = client.CancelOrder(market, order_index)
                if status < 0:
                    print >> f, "error_g: " + sAccountNo
                    print "error_g" + sAccountNo
                else:
                    print content.decode('GBK')

        orderlist = []
        row_index = 0
        while row_index < total_rows:  # 特定账户内，对股票列表进行循环
            time.sleep(0.05)
            Nowprice = 0.0
            Yesprice = 0.0
            Sendprice = 0.0
            buysell = 0  # 标记是买入还是卖出, 0是买入，1是卖出。
            Buymoney = 0.0  # 实际持仓与目标持仓的金额差异。
            Sendamount = 0  # 本次拟下单数量（单位为：股）。
            Nowamount = 0  # 本来就持有的Stockname的数量。
            Nowpercent = 0.0  # 每次循环之前赋给0.
            Stockname = Stock_buy.iloc[row_index, 0]
            Targetpercent = float(Stock_buy.iloc[row_index, 1])
            x_price = Stock_buy.iloc[row_index, 2]
            y_price = Stock_buy.iloc[row_index, 3]
            speed = Stock_buy.iloc[row_index, 4]
            buysell_1 = Stock_buy.iloc[row_index, 6]
            # speed的数值，比如0.2，代表将买五或卖五合计数量的0.2倍, 体现了买卖的力度和速率
            # [x_price, y_price]表示限价区间，买卖价不超过这个范围；
            # 若不修改，默认设为[0，1000.0]，则等同于不限价。
            # 即Sendprice小于x_price，或者Sendprice大于y_price，则不下单继续循环等待。
            # buysell_1用作读取result_send.csv里面的买卖方向。如果不为0或1，则最终下单以它为准。券商服务器一般默认buysell参数是1为卖，0为买。

            # status, content = client.GetQuote((Stockname))
            # if status < 0:
            #     print "error_d: " + content.decode('GBK')
            # else:
            #     # print content.decode('GBK')
            #     msvcrt.getch()
            #     temp1 = content.split('\n')
            #     temp2 = temp1[1].split('\t')
            #     Nowprice = float(temp2[5])  # nQsid=36, temp2[5]为当前价
            #     Yesprice = float(temp2[2])  # 昨收价
            #     if nQsid in [36, 16, 32, 28]:
            #         buy_5p = temp2[6:11]  # 买一至买五的价格，string,[0]是买一价
            #         buy_5v = temp2[11:16]  # 买五量，string，[0]是买一量
            #         sell_5p = temp2[16:21]  # 卖五价格，string,[4]是卖五价
            #         sell_5v = temp2[21:26]  # 卖五量，string
            #     if nQsid == 43:  # 华林
            #         buy_5p = temp2[6:9] + temp2[18:20]  # 买一至买五的价格，buy_5p[0]是买一价
            #         buy_5v = temp2[9:12] + temp2[20:22]  # 买五量，string，buy_5v[0]是买一量
            #         sell_5p = temp2[12:15] + temp2[22:24]  # 卖五价格，sell_5p[4]是卖五价
            #         sell_5v = temp2[15:18] + temp2[24:26]  # 卖五量，sell_5v[4] 卖五量

            if Stockname[0] in ['5', '6']:
                errinfo, count, result = clientHq.GetSecurityQuotes([(1, Stockname)])
            if Stockname[0] in ['0', '1', '2', '3']:
                errinfo, count, result = clientHq.GetSecurityQuotes([(0, Stockname)])
            if errinfo != "":
                print >> f, errinfo
            else:

                temp1 = result.split("\n")
                temp2 = temp1[1].split('\t')

                Nowprice = float(temp2[3])
                Yesprice = float(temp2[4])

                buy_5p = [temp2[17]] + [temp2[21]] + [temp2[25]] + [temp2[29]] + [temp2[33]]  # 买一至买五的价格，buy_5p[0]是买一价
                buy_5v = [temp2[19]] + [temp2[23]] + [temp2[27]] + [temp2[31]] + [temp2[35]]  # 买五量，string，buy_5v[0]是买一量
                sell_5p = [temp2[18]] + [temp2[22]] + [temp2[26]] + [temp2[30]] + [temp2[34]]  # 卖五价格，sell_5p[4]是卖五价
                sell_5v = [temp2[20]] + [temp2[24]] + [temp2[28]] + [temp2[32]] + [temp2[36]]  # 卖五量，sell_5v[4] 卖五量

            Sendprice = Nowprice  # 发出指令默认价格为当前价，程序后面会调整为买五或卖五。

            if Stockname in StockValue.iloc[:, 0].values:
                Nowpercent = StockValue[StockValue.iloc[:, 0].values == Stockname].iloc[0, 1]
                Nowamount = StockValue[StockValue.iloc[:, 0].values == Stockname].iloc[0, 3]

            Buymoney = TotalAsset * (Targetpercent - Nowpercent) / 100.0  # Buymoney可正可负

            if Buymoney > Available_money > 15000.0:
                Buymoney = Available_money - 10000.0  # 账户剩余可用资金不低于1万。
                print >> f, "Buymoney > Available_money"

            if Buymoney > 0.0:
                buysell = 0
                Sendamount = int(Buymoney / Nowprice / 100.0) * 100
                Total_sell = 0
                for isell in sell_5v:
                    Total_sell = Total_sell + int(isell)  # 买一至买五合计挂单量
                if Sendamount > int(Total_sell * speed) * 100:
                    Sendamount = int(Total_sell * speed) * 100
                    # 如果买单量大于合计挂单量的speed倍数，则按照speed下单。
                if Sendamount > 990000:  # 最大下单量99万股
                    Sendamount = 990000
                if abs(Buymoney) < 15000.0:  # 如果调仓资金量太小，则取消。
                    Sendamount = 0
                if abs(Buymoney) > 15000.0 > Sendamount * Nowprice:
                    Sendamount = 0
                    print "Buy/Sell list is too small, please change <speed>. "
                    isok = 0
                    # 如果盘面单子太少，同时因为speed设置太小，导致下单金额少于15000，则下单取消。
                    # 此时可以考虑略调整speed。
                if Available_money < 15000.0:
                    Sendamount = 0  # 如果账户没钱，则买单取消。
                    print "no enough money"
                Sendprice = float(sell_5p[4])  # 如果是买，按照卖五价格下单，确保成交
                if Sendprice > Nowprice * 1.015:
                    Sendprice = Nowprice * 1.015  # 如果卖五价差太大，则按照1.5%的差距来计算。
                if Sendprice > Yesprice * 1.07:
                    Sendprice = Yesprice * 1.07
                    Sendamount = 0  # 如果涨幅过大超过7%，就不要再追买。
                if buysell_1 == 1:
                    Sendamount = 0
                    print "buymoney > 0, but buysell_1 =1."
                # 如果本来是希望卖,从result_send里面读取到的buysell_1为1，则下单取消。
                if Sendprice < x_price or Sendprice > y_price:
                    isok = 0
                    Sendamount = 0
                    print "Sendprice isn't in the range of [x_price, y_price], Sendamount = 0"
                    # 如果出现sendprice超出[x_price, y_price]区间，可能会导致sendamount_all=0但同时任务没完成。
                    # 这个问题用 isok 来标记，如果isok 触发，则任务因为价格不再区间而未完成。

            if Buymoney < 0.0:
                buysell = 1  # 发出卖出指令
                Sendamount = - int(Buymoney / Nowprice / 100.0) * 100
                if abs(Buymoney) < 15000:  # 如果调仓资金量太小，则取消。
                    Sendamount = 0
                if Nowamount <= 100 or Nowamount * Nowprice < 15000.0:  # 如果账号还剩碎股或零头金额
                    Sendamount = int(Nowamount)
                if Sendamount > Nowamount:  # 如果卖单量大于可售量
                    Sendamount = int(Nowamount)
                Total_buy = 0
                for ibuy in buy_5v:
                    Total_buy = Total_buy + int(ibuy)  # 卖一至卖五合计挂单量
                if Sendamount > int(Total_buy * speed) * 100:
                    Sendamount = int(Total_buy * speed) * 100  # 如果买单量大于合计挂单量的speed倍，则按照speed下单。
                Sendprice = float(buy_5p[4])  # 如果是卖出，按照买五价格下单卖出，确保成交
                if abs(Buymoney) > 15000.0 > Sendamount * Nowprice:
                    Sendamount = 0
                    print "Maybe Buy/Sell list is too small, please check <speed>. "
                    isok = 0
                    # 如果盘面单子太少，同时因为speed设置太小，导致下单金额少于15000，则下单取消。
                    # 此时可以考虑略调整speed。
                if Sendamount > 990000:  # 最大下单量99万股
                    Sendamount = 990000  # 20180507: 这两行是新加的，之前只在买入时有这个，卖505888出问题。
                if Sendprice < Nowprice * 0.985:
                    Sendprice = Nowprice * 0.985  # 如果买五价差太大，则按照1.5%的差距来下单。
                # if Sendprice < Yesprice * 0.91:
                #     Sendprice = Yesprice * 0.91
                #     Sendamount = 0  # 如果下单价格低于跌9%,则下单取消。
                # 20180514注释掉上述三行，在[x_price, y_price]控制的背景下，这三行没用反而添乱。
                if buysell_1 == 0:
                    Sendamount = 0
                    print "Buymoney < 0, but buysell_1 = 0."
                    # 如果本来是希望买,从result_send里面读取到的buysell_1为0，则下单取消。
                if Sendprice < x_price or Sendprice > y_price:
                    isok = 0
                    Sendamount = 0
                    print "Sendprice isn't in the range of [x_price, y_price], Sendamount = 0"
                    # 如果出现sendprice超出[x_price, y_price]区间，可能会导致sendamount_all=0但同时任务没完成。
                    # 这个问题用 isok 来标记，如果isok 触发，则任务因为价格不再区间而未完成。

            print >> f, '-------------------------------------------'
            print >> f, ("Stockname: {}  Nowprice:{:.2f}   Buymoney: {:.2f}  Sendamount:{} "
                         .format(Stockname, Nowprice, Buymoney, Sendamount))
            print ("Stockname: {}  Nowprice:{:.2f}   Buymoney: {:.2f}  Sendamount:{} "
                   .format(Stockname, Nowprice, Buymoney, Sendamount))
            Sendamount_all = Sendamount_all + Sendamount

            if abs(Buymoney) > 10000000.0:
                print >> f, "Buymoney 大于1000万，异常"
                print "Buymoney 大于1000万，异常"

            if Sendamount > 0 and Stockname[0] in ['5', '6']:
                sTradeAccountNo = sTradeAccountNoshs[index]
                status, content = client.SendOrder(buysell, 0, sTradeAccountNo, Stockname, Sendprice, Sendamount)
                content = content.decode('GBK')
                if status < 0:
                    print >> f, "error_e: "
                    print content
                else:
                    print content
                    temp1 = content.split('\n')
                    temp2 = temp1[1].split('\t')
                    orderlist.append(str(temp2[0]))

            if Sendamount > 0 and Stockname[0] in ['0', '1', '2', '3']:
                status, content = client.SendOrder(buysell, 0, sTradeAccountNosz, Stockname, Sendprice, Sendamount)
                content = content.decode('GBK')
                if status < 0:
                    print >> f, "error_f: "
                    print content
                else:
                    print content
                    temp1 = content.split('\n')
                    temp2 = temp1[1].split('\t')
                    orderlist.append(str(temp2[0]))

            row_index = row_index + 1  # 之前是for stockname in Stockbuy, 相同stockname出现不止1次，则故障。

        time.sleep(sleeptime)
        # 这里查询今日委托，对本程序发出的 orderlist[]中的委托成交，追加到result_weituo，在程序末尾输出保存。
        # 这里最好修改orderlist[]的运用方式，最好是存orderlist，然后最终收盘后统一查询成交情况。
        # 这样可以在此处sleep较短的时间也无所谓。
        Category = 2
        status, content = client.QueryData(Category)
        if status < 0:
            print >> f, "error_c: "
        else:
            # print content.decode('GBK')
            temp1 = content.split('\n')
            len12 = len(temp1)
            for i_stock in range(1, len12):
                temp2 = temp1[i_stock].split('\t')
                if nQsid == 36:  # 中泰
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[8]  # 委托编号
                    cjsl = int(temp2[9])  # 成交数量
                    cjje = float(temp2[10])  # 成交金额
                    cjjj = float(temp2[11])
                    mmfx = int(temp2[4])
                    zjzh = temp2[17]  # 资金账号
                if nQsid == 16:  # 招商
                    daima = temp2[10]  # 股票代码
                    bianhao = temp2[9]  # 委托编号
                    cjsl = int(temp2[6])  # 成交数量
                    cjje = float(temp2[5]) * cjsl  # 成交金额
                    cjjj = float(temp2[5])  # 成交价格
                    mmfx = int(temp2[2])
                    zjzh = temp2[11]  # 注意这里返回的是股东代码
                if nQsid == 43:  # 华林
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[9]  # 委托编号
                    cjsl = int(temp2[10])  # 成交数量
                    cjje = float(temp2[11])  # 成交金额
                    if cjsl == 0:
                        cjjj = 0
                    elif abs(cjsl > 0):
                        cjjj = cjje / cjsl  # 成交均价
                    mmfx = int(temp2[4])  # 买卖方向
                    zjzh = temp2[15]  # 注意这里返回的是股东代码
                if nQsid == 32:  # 中信
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[19]  # 委托编号
                    cjsl = int(float(temp2[11]))  # 成交数量
                    cjje = float(temp2[12])  # 成交金额
                    cjjj = float(temp2[10])  # 成交价格
                    mmfx = int(temp2[4])  # 买卖标志
                    zjzh = temp2[18]  # 资金账号
                if nQsid == 28:  # 光大
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[13]  # 委托编号
                    cjsl = int(temp2[9])  # 成交数量
                    cjjj = float(temp2[8])  # 成交均价
                    cjje = cjsl * cjjj  # 成交金额
                    mmfx = int(temp2[5])  # 买卖标志
                    zjzh = temp2[14]  # 资金账号,光大返回的数据是股东代码
                if bianhao in orderlist:
                    Weituo_ins.iat[0, 0] = daima  # 股票代码
                    Weituo_ins.iat[0, 1] = bianhao  # 委托编号
                    Weituo_ins.iat[0, 2] = cjsl  # 该委托的成交数量
                    Weituo_ins.iat[0, 3] = cjje  # 该委托的成交金额
                    Weituo_ins.iat[0, 4] = cjjj  # 该委托的成交均价
                    Weituo_ins.iat[0, 5] = mmfx  # 该委托的买卖方向，通常0买1卖。
                    Weituo_ins.iat[0, 6] = zjzh  # 该委托的资金账号
                    Weituo_ins.iat[0, 7] = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
                    Weituo_here = pd.concat([Weituo_here, Weituo_ins], axis=0, ignore_index=True)

        Sendamount_all = Sendamount_all + Sendamount
        index = index + 1
        # time.sleep(sleeptime)  # 稍后，登录下一个账号。
        del client

    S_L = StockValue_all
    SL1 = S_L.sort_values(['stocknum'])
    SL2 = SL1.drop_duplicates(subset='stocknum')
    SL3 = SL2.drop('a')
    SL4 = SL3.drop(['account', 'ratio', 'asset', 'amount'], axis=1)
    indexhere = account_num - 1
    while indexhere >= 0:
        S_L_index = S_L.ix[S_L.account == sAccountNos[indexhere], [0, 1]]
        SL4 = pd.merge(S_L_index, SL4, how='outer', on='stocknum')
        indexhere = indexhere - 1
    if account_num > 3:  # 产品
        SL4.columns = ['stocknum', 'H3_HL', 'H6_ZS', 'H9_ZT', 'H6_ZT', 'H9_ZX', 'H6_GD']
    if account_num <= 3:  # 委外
        SL4.columns = ['stocknum', 'DM_ZT', 'RY_ZT']
    # exlist = list(SL4.stocknum)
    # exlist.remove('SHXGED')
    # exlist.remove('SZXGED')
    # SL4 = SL4[SL4.stocknum.isin(exlist)]
    # 这一段出现exlist的语句，用于删除股票代码为888880、SHXGED、SZXGED这几个无效品种

    account_num1 = len(list(SL4.stocknum))
    for acc_index in range(0, account_num1):
        if str(SL4.iloc[acc_index, 0]).startswith(('6', '5')):
            SL4.iloc[acc_index, 0] = '.'.join([str(SL4.iloc[acc_index, 0]), 'SH'])
        if str(SL4.iloc[acc_index, 0]).startswith(('0', '1', '2', '3')):
            SL4.iloc[acc_index, 0] = '.'.join([str(SL4.iloc[acc_index, 0]), 'SZ'])
    DataFrame(SL4).to_excel("portfolio.xlsx", sheet_name="all", index=True, header=True)

    stock_len = len(list(Weituo_here.stockname))
    for acc_index in range(0, stock_len):
        if str(Weituo_here.iloc[acc_index, 0]).startswith(('6', '5')):
            Weituo_here.iloc[acc_index, 0] = '.'.join([str(Weituo_here.iloc[acc_index, 0]), 'SH'])
        if str(Weituo_here.iloc[acc_index, 0]).startswith(('0', '1', '2', '3')):
            Weituo_here.iloc[acc_index, 0] = '.'.join([str(Weituo_here.iloc[acc_index, 0]), 'SZ'])
        Weituo_here.iloc[acc_index, 6] = '.'.join([str(Weituo_here.iloc[acc_index, 6]), 'S'])
    DataFrame(Weituo_here).to_excel("result_weituo.xlsx", sheet_name="all", index=True, header=True)

    if Sendamount_all == 0 and isok == 1:
        print "timeindex =    " + str(timeindex)
        print >> f, "timeindex =    " + str(timeindex)
        print >> f, time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        print time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        print "All accounts have been adjusted the ratio defined in result_send.csv. Finished!"
        print >> f, "All accounts have been adjusted the ratio defined in result_send.csv. Finished!"
        break
    else:
        print "All accounts have run once, wait for several minutes for the next run: "
        print >> f, "Every account have run once, wait for several minutes for the next run: "
        print "timeindex =    " + str(timeindex)
        print >> f, "timeindex =    " + str(timeindex)
        print "Begin wait X minutes... Please see result_log.txt to know detail!"
        print >> f, "Begin wait X minutes... Please see result_log.txt to know detail!"
        print >> f, time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        print time.strftime("%Y%m%d %H:%M:%S", time.localtime())

    timeindex = timeindex + 1
    f.close()
    TradeX.CloseTdx()
    if account_num == 7:
        time.sleep(sleeptime)
    if account_num == 2:
        time.sleep(sleeptime*6)
    # 账户少的程序组，等一等账户多的程序组。