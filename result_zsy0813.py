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
import xlwings as xw
import datetime



managerslist=['zhoutao','xiaodong','xiaoxin','zhengshuoyu']
managers_posaddlist=['./zt/zhoutao/Position_zhoutao.xlsx','./xd/xiaodong/Position_xiaodong.xlsx','./xx/xiaoxin/Position_xiaoxin.xlsx','./zsy/zhengshuoyu/Position_zheng.xlsx']
managers_sendaddlist=['./zt/zhoutao/result_send_zhoutao.csv','./xd/xiaodong/result_send_xiaodong.csv','./xx/xiaoxin/result_send_xiaoxin.csv','./zsy/zhengshuoyu/result_send_zheng.csv']
result_weituolist=['./zt/zhoutao/result_weituo_zhoutao.xlsx','./xd/xiaodong/result_weituo_xiaodong.xlsx','./xx/xiaoxin/result_weituo_xiaoxin.xlsx','./zsy/zhengshuoyu/result_weituo_zheng.xlsx']
result_weituo_total=['./result_weituo_num.xlsx','./result_weituo_num_origin.xlsx']

managers_posaddlist_backup=['./zt/Position_zhoutao.xlsx','./xd/Position_xiaodong.xlsx','./xx/Position_xiaoxin.xlsx','./zsy/Position_zheng.xlsx']
result_weituolist_backup=['./zt/result_weituo_zhoutao.xlsx','./xd/result_weituo_xiaodong.xlsx','./xx/result_weituo_xiaoxin.xlsx','./zsy/result_weituo_zheng.xlsx']

managernum=len(managerslist)


# 此处读取account.xlsx文件里面的账户信息。
Account = pd.read_excel("account.xlsx")
account_num = len(list(Account.nQsid))

#读取行情ip地址和端口
HostHq = pd.read_excel("HostHq.xlsx")
HostHq_num = len(list(HostHq.port))


#如果增加账户，需要更改这里



for inx in range(0, account_num):
    Account.iloc[inx, 7] = Account.sAccountNo[inx].split('.')[0]  # 把.S的后缀去掉.
    Account.iloc[inx, 9] = Account.sTradeAccountNosz[inx].split('.')[0]  # 把.S的后缀去掉.
    Account.iloc[inx, 10] = Account.sPassword[inx].split('.')[0]  # 把.S的后缀去掉.
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


Weituo_here = pd.read_excel(result_weituo_total[0])
Weituo_here=Weituo_here.reset_index(drop=True)

for inx in range(0, len(Weituo_here.stockname)):
    Weituo_here.iloc[inx, 0] = Weituo_here.stockname[inx].split('.')[0]  # 把.SZ和.SH的字样去掉.
    Weituo_here.iloc[inx, 6] = Weituo_here.account[inx].split('.')[0]  # 把account的.S字样去掉.
Weituo_ins = pd.DataFrame({'stockname': ['00000.SZ'], 'bianhao': ['100'], 'amount': [0], 'money': [0.0], 'price': [0.0], 'buysell': [0], 'account': ['1092.S'], 'time': ['11:08'],'manager':['xxx'],'managerweight':['0.0'],'dealflag':['no']})
Weituo_ins = Weituo_ins[['stockname', 'bianhao', 'amount', 'money', 'price', 'buysell', 'account', 'time','manager','managerweight','dealflag']]

timeindex = 1  # 记录while 1这个循环的循环次数
isok = 0  # 记录是否完成调仓任务, 0表示任务未完成， 1表示任务完成。


while 1:
    
    starttime = datetime.datetime.now()
    hold_amount_eachaccounts=[]
    cashcolumns=['manager','ZXcash','ZTcash']
    cash=DataFrame(np.arange(managernum*(account_num+1)).reshape(managernum,account_num+1),columns=cashcolumns)
    
    result_log = open("result_log_num.txt", 'a')
    print "\nWelcome to the trading system of Heisen Asset Management!"
    print >>result_log,'**************************************************'
    print >>result_log,'**************************************************'
    print >> result_log, "\nWelcome to the trading system of Heisen Asset Management!"
    print >>result_log,'timeindex:  '+str(timeindex)
    print >>result_log,time.strftime("%Y%m%d %H:%M:%S", time.localtime())
    
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
        time.sleep(10)
        continue  # 如果非交易时间测试下单程序，需要#掉这个continue语句。
        
###############################################################################
###修改#################
    #生成下单列表
    Stock_buy_columns=['stockname','target_num','x_price','y_price','speed','sleeptime','buysell','name','manager']
    
    Stock_buy=pd.DataFrame(columns=Stock_buy_columns)  
    
   
    for manager_i in range(0,managernum):
        managers_sendadd=managers_sendaddlist[manager_i]
        manager=managerslist[manager_i]
        if os.path.exists(managers_sendadd):
            Stock_buyeach=pd.DataFrame(columns=Stock_buy_columns)  
            Stock_buy_zt0=read_csv(managers_sendadd, dtype={'stockname': str})
            Stock_buyeach=pd.concat([Stock_buyeach,Stock_buy_zt0],ignore_index=True) 
            for i in range(0,len(Stock_buyeach)):
                Stock_buyeach.manager.iloc[i]=manager
                SHSZ=Stock_buyeach.stockname.iloc[i].split('.')[1]
                SHSZstockname=Stock_buyeach.stockname.iloc[i].split('.')[0]
                if SHSZstockname[0] in ['5','6']:
                    if SHSZ!='SH':
                        print >>result_log,str(manager)+' :SH or SZ is wrong!!'
                        print '--------------------------------------------'
                        print str(manager)+' :SH or SZ is wrong!!'
                        Stock_buyeach=pd.DataFrame(columns=Stock_buy_columns) 
                        break
                if SHSZstockname[0] in ['0', '1', '2', '3']:
                    if SHSZ!='SZ':
                        print >>result_log,str(manager)+' :SH or SZ is wrong!!'
                        print '--------------------------------------------'
                        print str(manager)+' :SH or SZ is wrong!!'
                        Stock_buyeach=pd.DataFrame(columns=Stock_buy_columns)  
                        break
                
            #Stock_buy是股票的下单目标数量DataFrame
            Stock_buy=pd.concat([Stock_buy,Stock_buyeach],ignore_index=True) 
        
        managers_posadd=managers_posaddlist[manager_i]
        hold_amount_eachaccounttmp=pd.read_excel(managers_posadd,sheetname='Sheet2')
        hold_amount_eachaccounttmp=hold_amount_eachaccounttmp.reset_index(drop=True)
        hold_amount_eachaccounts.append(hold_amount_eachaccounttmp)
        cash.iloc[manager_i,0]=manager
        cash.iloc[manager_i,1]=hold_amount_eachaccounttmp.iloc[1,2]
        cash.iloc[manager_i,2]=hold_amount_eachaccounttmp.iloc[1,4]
    
    Stock_buy=Stock_buy.loc[:,Stock_buy_columns]
    print '--------------Stock_buy---------------------'
    print Stock_buy
    print >>result_log,'--------------Stock_buy---------------------'
    print >>result_log,Stock_buy
    print >>result_log,'-----------hold_amount_eachaccounts------------------'
    print >>result_log,hold_amount_eachaccounts
  
    #Send_amount这个DataFrame记录需要买卖的数量
    Stock_buylen1=len(Stock_buy)
    Send_amount_columns=['stockname','manager','ZX','ZT']
    Send_amount = DataFrame(np.zeros((Stock_buylen1, account_num+2)),columns=Send_amount_columns)
   
    Asset_total=0
    Asset_ZX=0
    Asset_ZT=0
    x=0
    #对下单股票列表Stock_buy进行循环，计算需要买卖的数量
    for i in range(0,Stock_buylen1):
        send_amount_total=0
        send_amount_ZX00=0
        send_amount_ZT00=0
        stockname_buy=Stock_buy.stockname.iloc[i]
        target_num_buy=Stock_buy.target_num.iloc[i]
        manager=Stock_buy.manager.iloc[i]
        
        #在Stock_buy循环的时候要判断是下单指令来自于哪一个投资经理
        if manager in managerslist:
            manager_i=managerslist.index(manager)
            hold_amount_eachaccount=hold_amount_eachaccounts[manager_i]
            
        #已经知道这条指令来自该投资经理，如果这个投资经理以前已经持有该股票
        if stockname_buy in hold_amount_eachaccount.stockname.values:
                hold_amount_total = hold_amount_eachaccount[hold_amount_eachaccount.stockname == stockname_buy].iloc[0, 1]
                send_amount_total=Stock_buy.target_num.iloc[i]-hold_amount_total
                
               
                
                #这个地方以后要扩展，现在是两个账户，以后会有多个账户
                hold_amount_ZX00=hold_amount_eachaccount[hold_amount_eachaccount.stockname== stockname_buy].iloc[0, 2]
                hold_amount_ZT00=hold_amount_eachaccount[hold_amount_eachaccount.stockname== stockname_buy].iloc[0, 4]
                available_amount_ZX00=hold_amount_eachaccount[hold_amount_eachaccount.stockname== stockname_buy].iloc[0, 3]
                available_amount_ZT00=hold_amount_eachaccount[hold_amount_eachaccount.stockname== stockname_buy].iloc[0, 5]
                
                #对于已经持有，现在要卖出去的情况，
                #要判断卖出的下单量是否已经超过自身的可卖数量,如果超过可卖数量，将该股票清仓
                if send_amount_total<0:
                    x=float(hold_amount_ZX00)/hold_amount_total
                    
                    send_amount_ZX00=int(send_amount_total*x/100.0)*100
                    send_amount_ZT00=send_amount_total-send_amount_ZX00
                   
                    if available_amount_ZT00<-send_amount_ZT00:
                        send_amount_ZT00=-available_amount_ZT00
                    if available_amount_ZX00<-send_amount_ZX00:
                        send_amount_ZX00=-available_amount_ZX00
                     
                    #清仓的情况
                    if Stock_buy.target_num.iloc[i]<100:
                        send_amount_ZT00=-available_amount_ZT00
                        send_amount_ZX00=-available_amount_ZX00
                #对于已经持有，现在还要继续买入的情况，要判断买入的金额是否已经超过自己的可用资金
                #这里判断的时候要用到股票的价格数据，因此要在登录账户的程序部分更改
                else:
                    Asset_total=hold_amount_eachaccount.iloc[1,1]
                    Asset_ZX=hold_amount_eachaccount.iloc[1,2]
                    Asset_ZT=hold_amount_eachaccount.iloc[1,4]
                    x=float(Asset_ZX)/Asset_total
                    send_amount_ZX00=int(send_amount_total*x/100.0)*100
                    send_amount_ZT00=send_amount_total-send_amount_ZX00
                    
                    
        #如果是新开仓该股票，新开仓股票需要两个账户的资金比例
        #Asset_total是两个账户的总资金
        #Asset_ZX是中信账户的总资金
        else:
            send_amount_total=Stock_buy.target_num.iloc[i]
            Asset_total=hold_amount_eachaccount.iloc[1,1]
            Asset_ZX=hold_amount_eachaccount.iloc[1,2]
            Asset_ZT=hold_amount_eachaccount.iloc[1,4]
            x=float(Asset_ZX)/Asset_total
            send_amount_ZX00=int(send_amount_total*x/100.0)*100
            send_amount_ZT00=send_amount_total-send_amount_ZX00
           
        #生成Send_amount
        Send_amount.stockname.iloc[i]=stockname_buy
        Send_amount.manager.iloc[i]=manager
        Send_amount.ZX.iloc[i]=send_amount_ZX00
        Send_amount.ZT.iloc[i]=send_amount_ZT00
        
    print '-------------send_amount_total-----------------'
    print Send_amount
    
    print >> result_log,'-------------send_amount_total-----------------'
    print >> result_log,Send_amount
   
################################################################################

    # 从csv文件读取建仓目标
    sleeptime = Stock_buy.ix[0, 'sleeptime']
    # 只读取调用了第一行数据的“sleeptime”数值。sleeptime是对股票账户循环过程中的等待时间。
    # 目前是“股票循环”内嵌于“账号循环”，暂无法对不同的股票设置不同的sleeptime.

    for inx in range(0, len(Stock_buy.stockname)):
        Stock_buy.stockname.iloc[inx] = Stock_buy.stockname[inx].split('.')[0]  # 把.SZ和.SH的字样去掉.

    total_rows = len(Stock_buy)
    #  用作标记Stock_buy的有效数据行数


    TradeX.OpenTdx(14, "6.40", 12, 0)
    
    #HostHq=["221.231.141.60", "7709"]
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
    
    #clientHq = TradeX.TdxHq_Connect(HostHq.iloc[0,0], int(HostHq.iloc[0,1]))

    StockValue_all = DataFrame(np.zeros((1, 5)), index=['a'], columns=['stocknum', 'ratio', 'asset', 'amount', 'account'])
    # StockValue_all用于存储各个账号的持
    #Accountasset_all用于存储各个账号的资产
    Accountasset_all = DataFrame(columns=['account', 'asset'])
    
    index = 0
    Sendamount_all = 0
    
    ##################################################################
    ###对账户进行循环
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
        
        print >>result_log,'------------sAccountNo---------------'
        ####修改账号需要改变的地方###############################
        ########################################################
        #########################################################
        if sAccountNo == str(sAccountNos[0]):  # 中信
            Send_amount_account=Send_amount.ZX
            
        elif sAccountNo == str(sAccountNos[1]):  # 中泰
            Send_amount_account=Send_amount.ZT
        ############################################################
        #######################################################
        #######################################################

        
        print >>result_log,"login account:  " + sAccountNo
        try:
            client = TradeX.Logon(nQsid, sHost, nPort, sVersion, nBranchID,
                                  nAccountType, sAccountNo, sTradeAccountNo,
                                  sPassword, sTxPassword)
        except TradeX.error, e:
            print e.message
            print ("nQsid: {}  nHost:{}   nPort: {}  sVersion:{} "
                   "nBranchID:{} nAccountType:{} sAcccountNo:{} sTradeAccountNo:{}"
                   .format(nQsid, sHost, nPort, sVersion, nBranchID, nAccountType, sAccountNo, sTradeAccountNo))
            TradeX.CloseTdx()
            sys.exit(-1)

       ###################################################################
       ##查询资金
        nCategory = 0
        status, content = client.QueryData(nCategory)
        if status < 0:
            #print >> f, "error_b: " + str(content)
            print >>result_log,"error_b" + content.decode('GBK').encode('utf8')
        else:
            print >>result_log,content.decode('GBK').encode('utf8')
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
        ###########################################################################

        ############################################################################
        ##查询股份
        Category = 1  # 查询股份
        status, content = client.QueryData(Category)
        if status < 0:
            print "error_c: "
        else:
            # print content.decode('GBK')
            temp1 = content.split('\n')
            len12 = len(temp1)
            StockValue = DataFrame(np.zeros((len12 - 1, 5)),
                                   columns=['stocknum', 'ratio', 'asset', 'amount', 'account'])
            Accountasset= DataFrame(np.zeros((1, 2)), columns=['account', 'asset'])
            
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
                    StockValue.iloc[i - 1, 2] = float(temp2[8])  # 最新市值
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
            #####################################
            Accountasset.iloc[0,0]=sAccountNo
            Accountasset.iloc[0,1]=TotalAsset
            Accountasset_all=pd.concat([Accountasset_all,Accountasset])
            ###########################################
            #print >> f, "Current Portfolio of the Account: " + str(sAccountNo)
            # print "Current Portfolio of the Account: " + sAccountNo
            #print >> f, str(StockValue)
            # print StockValue
            
        
        ############################################################################################
        ##查询可撤单
        Category = 4  # 查询可撤单
        status, content = client.QueryData(Category)
        if status < 0:
            print "error_c: "
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
                    #print >> f, "error_g: " + sAccountNo
                    print >>result_log,"error_g" + sAccountNo
                else:
                    print >>result_log,content.decode('GBK').encode('utf8')
        #########################################################################################
            
            
        orderlist_zt=[]
        orderlist_xd=[]
        orderlist_xx=[]
        orderlist_zh=[]
        row_index = 0
        while row_index < total_rows:  # 特定账户内，对股票列表进行循环
            time.sleep(0.05)
            Nowprice = 0.0
            Yesprice = 0.0
            Sendprice = 0.0
            buysell = 0  # 标记是买入还是卖出, 0是买入，1是卖出。
            Buyamount = 0.0  # 实际持仓与目标持仓的金额差异。
            Sendamount = 0  # 本次拟下单数量（单位为：股）。
            Nowamount = 0  # 本来就持有的Stockname的数量。
            Nowpercent = 0.0  # 每次循环之前赋给0.
            
            ##################################################
            #######修改此处#######################
            
            Stockname = Stock_buy.stockname.iloc[row_index]
            #Targetpercent = float(Stock_buy.percent.iloc[row_index])
            x_price = Stock_buy.x_price.iloc[row_index]
            y_price = Stock_buy.y_price.iloc[row_index]
            speed = Stock_buy.speed.iloc[row_index]
            buysell_1 = Stock_buy.buysell.iloc[row_index]
            manager=Stock_buy.manager.iloc[row_index]
            Sendamount=Send_amount_account.iloc[row_index]
            
            
            #####################################################
            
            
            # speed的数值，比如0.2，代表将买五或卖五合计数量的0.2倍, 体现了买卖的力度和速率
            # [x_price, y_price]表示限价区间，买卖价不超过这个范围；
            # 若不修改，默认设为[0，1000.0]，则等同于不限价。
            # 即Sendprice小于x_price，或者Sendprice大于y_price，则不下单继续循环等待。
            # buysell_1用作读取result_send.csv里面的买卖方向。如果不为0或1，则最终下单以它为准。券商服务器一般默认buysell参数是1为卖，0为买。
            
            if Stockname[0] in ['5', '6']:
                errinfo, count, result = clientHq.GetSecurityQuotes([(1,Stockname)])
            if Stockname[0] in ['0', '1', '2', '3']:
                errinfo, count, result = clientHq.GetSecurityQuotes([(0,Stockname)])
            if errinfo != "":
                print >>result_log,errinfo
            else:
                
                temp1=result.split("\n")
                temp2 = temp1[1].split('\t')
                
                Nowprice = float(temp2[3])  # nQsid=36, temp2[5]为当前价
                if Nowprice < 0.01:
                    print >> result_log, "\n Nowprice = 0, and continue to next stock"
                    continue  #如果查询到的nowprice=0，后面除法计算股票数量时会报错。出现=0，则跳出这轮循环。
                Yesprice = float(temp2[4])  # 昨收价
               
                buy_5p = [temp2[17]] + [temp2[21]]+[temp2[25]]+ [temp2[29]]+ [temp2[33]]  # 买一至买五的价格，buy_5p[0]是买一价
                buy_5v = [temp2[19]] + [temp2[23]]+[temp2[27]]+ [temp2[31]]+ [temp2[35]]  # 买五量，string，buy_5v[0]是买一量
                sell_5p = [temp2[18]] + [temp2[22]]+[temp2[26]]+ [temp2[30]]+ [temp2[34]]  # 卖五价格，sell_5p[4]是卖五价
                sell_5v = [temp2[20]] + [temp2[24]]+[temp2[28]]+ [temp2[32]]+ [temp2[36]]  # 卖五量，sell_5v[4] 卖五量
                
                print >>result_log,'-------------------------------------------------------'
                print >>result_log,buy_5p
                print >>result_log,buy_5v
                print >>result_log,sell_5p
                print >>result_log,sell_5v
                 
   
            '''status, content = client.GetQuote((Stockname))
            if status < 0:
                print "error_d: " + content.decode('GBK')
            else:
                # print content.decode('GBK')
                temp1 = content.split('\n')
                temp2 = temp1[1].split('\t')
                
                print >>result_log,temp2
                
                Nowprice = float(temp2[5])  # nQsid=36, temp2[5]为当前价
                Yesprice = float(temp2[2])  # 昨收价
                if nQsid in [36, 16, 32, 28]:
                    buy_5p = temp2[6:11]  # 买一至买五的价格，string,[0]是买一价
                    buy_5v = temp2[11:16]  # 买五量，string，[0]是买一量
                    sell_5p = temp2[16:21]  # 卖五价格，string,[4]是卖五价
                    sell_5v = temp2[21:26]  # 卖五量，string
                if nQsid == 43:  # 华林
                    buy_5p = temp2[6:9] + temp2[18:20]  # 买一至买五的价格，buy_5p[0]是买一价
                    buy_5v = temp2[9:12] + temp2[20:22]  # 买五量，string，buy_5v[0]是买一量
                    sell_5p = temp2[12:15] + temp2[22:24]  # 卖五价格，sell_5p[4]是卖五价
                    sell_5v = temp2[15:18] + temp2[24:26]  # 卖五量，sell_5v[4] 卖五量
'''
            Sendprice = Nowprice  # 发出指令默认价格为当前价，程序后面会调整为买五或卖五。
            
            print >>result_log,str(Stockname)+' nowprice is '+str(Nowprice)

            Buymoney=Sendamount*Nowprice
           

            if Sendamount > 0.0:
                #获取投资经理相应账户的现金，从而来判断买卖的金额是否超过自己的现金
                if manager in managerslist:
                    manager_i=managerslist.index(manager)
                    capitalcash=cash.iloc[manager_i,index+1]
                    
                
                buysell = 0
                Total_sell = 0
                for isell in sell_5v:
                    Total_sell = Total_sell + int(isell)  # 买一至买五合计挂单量
                
                if Sendamount > int(Total_sell * speed) * 100:
                    Sendamount = int(Total_sell * speed) * 100
                    # 如果买单量大于合计挂单量的speed倍数，则按照speed下单。
                    
                #买入时要判断买入的股票数量乘以股价是否超过投资经理的现金金额。
                if Buymoney>capitalcash:
                    Sendamount=0
                    print manager+': target_num maybe wrong, check!'
                    
                if Sendamount > 990000:  # 最大下单量99万股
                    Sendamount = 990000
               
                Sendprice = float(sell_5p[4])  # 如果是买，按照卖五价格下单，确保成交
                if Sendprice > Nowprice * 1.015:
                    Sendprice = Nowprice * 1.015  # 如果卖五价差太大，则按照1.5%的差距来计算。
                if Sendprice > Yesprice * 1.07:
                    Sendprice = Yesprice * 1.07
                    Sendamount = 0  # 如果涨幅过大超过7%，就不要再追买。
                    print 'rise more than 7%, give up buying '
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

            if Sendamount < 0.0:
                buysell = 1  # 发出卖出指令
                Sendamount=-Sendamount
                Total_buy = 0
                for ibuy in buy_5v:
                    Total_buy = Total_buy + int(ibuy)  # 卖一至卖五合计挂单量
                
                if Sendamount > int(Total_buy * speed) * 100:
                    Sendamount = int(Total_buy * speed) * 100  # 如果买单量大于合计挂单量的speed倍，则按照speed下单。
                Sendprice = float(buy_5p[4])  # 如果是卖出，按照买五价格下单卖出，确保成交
                '''if Sendamount * Nowprice<15000:
                    Sendamount = 0
                    print "Maybe Buy/Sell list is too small, please check <speed>. "
                    isok = 0'''
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

            print >>result_log,'----------------------------------------------------------------'
            print >>result_log,'accountno: '+str(index)+' manager: '+manager+' Stock: '+str(Stockname)+' Sendamount: '+str(Sendamount)
            print >>result_log,'----------------------------------------------------------------'
            
            print '----------------------------------------------------------------'
            print 'accountno: '+str(index)+' manager: '+manager+' Stock: '+str(Stockname)+' Sendamount: '+str(Sendamount)
            print '----------------------------------------------------------------'

            if abs(Buymoney) > 10000000.0:
                #print >> f, "Buymoney 大于1000万，异常"
                print "Buymoney 大于1000万，异常"

            if Sendamount > 0 and Stockname[0] in ['5', '6']:
                sTradeAccountNo = sTradeAccountNoshs[index]
                status, content = client.SendOrder(buysell, 0, sTradeAccountNo, Stockname, Sendprice, int(Sendamount))
                time.sleep(1)
                content = content.decode('GBK').encode('utf8')
                if status < 0:
                    print >> result_log, "error in SendOrder "
                    print >> result_log,content
                    print "error in SendOrder "
                    print content
                else:
                    print content
                    temp1 = content.split('\n')
                    temp2 = temp1[1].split('\t')
                    
                    ######修改此处#########################
                    if manager=='zhoutao':
                        orderlist_zt.append(str(temp2[0]))
                    if manager=='xiaodong':
                        orderlist_xd.append(str(temp2[0]))
                    if manager=='xiaoxin':
                        orderlist_xx.append(str(temp2[0]))
                    if manager=='zhengshuoyu':
                        orderlist_zh.append(str(temp2[0]))
                    #######################################

            if Sendamount > 0 and Stockname[0] in ['0', '1', '2', '3']:
                status, content = client.SendOrder(buysell, 0, sTradeAccountNosz, Stockname, Sendprice, int(Sendamount))
                time.sleep(1)
                content = content.decode('GBK').encode('utf8')
                if status < 0:
                    print >> result_log, "error in SendOrder "
                    print >> result_log,content
                    print "error in SendOrder "
                    print content
                else:
                    print content
                    temp1 = content.split('\n')
                    temp2 = temp1[1].split('\t')
                    
                    ######修改此处#############################
                    if manager=='zhoutao':
                        orderlist_zt.append(str(temp2[0]))
                    if manager=='xiaodong':
                        orderlist_xd.append(str(temp2[0]))
                    if manager=='xiaoxin':
                        orderlist_xx.append(str(temp2[0]))
                    if manager=='zhengshuoyu':
                        orderlist_zh.append(str(temp2[0]))
                    ############################################

            row_index = row_index + 1  # 之前是for stockname in Stockbuy, 相同stockname出现不止1次，则故障。

        time.sleep(15)
        
        ############################################################################################
        ##查询委托之后立刻查询可撤单
        Category = 4  # 查询可撤单
        status, content = client.QueryData(Category)
        
        if status < 0:
            print >>result_log,"error in afer_chedan "
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
                    #print >> f, "error_g: " + sAccountNo
                    print >>result_log,"error in afer_chedan" + sAccountNo
                else:
                    print >>result_log,"chedan success"
        
        
        #########################################################################################
        # 这里查询今日委托，对本程序发出的 orderlist[]中的委托成交，追加到result_weituo，在程序末尾输出保存。
        # 这里最好修改orderlist[]的运用方式，最好是存orderlist，然后最终收盘后统一查询成交情况。
        # 这样可以在此处sleep较短的时间也无所谓。
        Category = 2
        status_weituo, content_weituo = client.QueryData(Category)
       
        ###查询委托之后的处理
        if status_weituo < 0:
            print "error in query_weituo"
        else:
            # print content.decode('GBK')
            temp1 = content_weituo.split('\n')
            len12 = len(temp1)
            for i_stock in range(1, len12):
                temp2 = temp1[i_stock].split('\t')
                ###################################################
                ####修改此处，nQsid不是账户的唯一标识，用资金账号
                #资金账号是该账户的唯一标记
                if sAccountNo == '0508490929': 
                    daima = temp2[10]  # 股票代码
                    bianhao = temp2[9]  # 委托编号
                    cjsl = int(temp2[6])  # 成交数量
                    cjje = float(temp2[5]) * cjsl  # 成交金额
                    cjjj = float(temp2[5])  # 成交价格
                    mmfx = int(temp2[2])
                    zjzh = sAccountNo  # 注意这里返回的是股东代码
                if sAccountNo == '21698049':  #
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[13]  # 委托编号
                    cjsl = int(temp2[9])  # 成交数量
                    cjjj = float(temp2[8])  # 成交均价
                    cjje = cjsl * cjjj  # 成交金额
                    mmfx = int(temp2[5])  # 买卖标志
                    zjzh = sAccountNo  # 资金账号,光大返回的数据是股东代码
                if sAccountNo == '109229000356':  # 中泰
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[8]  # 委托编号
                    cjsl = int(temp2[9])  # 成交数量
                    cjje = float(temp2[10])  # 成交金额
                    cjjj = float(temp2[11])
                    mmfx = int(temp2[4])
                    zjzh = sAccountNo  # 资金账号
                if sAccountNo == '109229000322':  # 中泰
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[8]  # 委托编号
                    cjsl = int(temp2[9])  # 成交数量
                    cjje = float(temp2[10])  # 成交金额
                    cjjj = float(temp2[11])
                    mmfx = int(temp2[4])
                    zjzh = sAccountNo  # 资金账号
                
                
                '''
                if nQsid == 36:  # 中泰
                    daima = temp2[2]  # 股票代码
                    bianhao = temp2[8]  # 委托编号
                    cjsl = int(temp2[9])  # 成交数量
                    cjje = float(temp2[10])  # 成交金额
                    cjjj = float(temp2[11])
                    mmfx = int(temp2[4])
                    zjzh = temp2[17]  # 资金账号
                
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
                    zjzh = temp2[18] 
            '''
                    
                
                #######修改此处############################################
                if bianhao in orderlist_zt:
                    Weituo_ins.iloc[0, 0] = daima  # 股票代码
                    Weituo_ins.iloc[0, 1] = bianhao  # 委托编号
                    Weituo_ins.iloc[0, 2] = cjsl  # 该委托的成交数量
                    Weituo_ins.iloc[0, 3] = cjje  # 该委托的成交金额
                    Weituo_ins.iloc[0, 4] = cjjj  # 该委托的成交均价
                    Weituo_ins.iloc[0, 5] = mmfx  # 该委托的买卖方向，通常0买1卖。
                    Weituo_ins.iloc[0, 6] = zjzh  # 该委托的资金账号
                    Weituo_ins.iloc[0, 7] = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
                    Weituo_ins.iloc[0,8]='zhoutao'
                    #Weituo_ins.iloc[0,9]=10000000.0/TotalAsset
                    Weituo_ins.iloc[0,9]=1.0
                    Weituo_ins.iloc[0,10]='no'
                    Weituo_here = pd.concat([Weituo_here, Weituo_ins],ignore_index=True)
                if bianhao in orderlist_xd:
                    Weituo_ins.iloc[0, 0] = daima  # 股票代码
                    Weituo_ins.iloc[0, 1] = bianhao  # 委托编号
                    Weituo_ins.iloc[0, 2] = cjsl  # 该委托的成交数量
                    Weituo_ins.iloc[0, 3] = cjje  # 该委托的成交金额
                    Weituo_ins.iloc[0, 4] = cjjj  # 该委托的成交均价
                    Weituo_ins.iloc[0, 5] = mmfx  # 该委托的买卖方向，通常0买1卖。
                    Weituo_ins.iloc[0, 6] = zjzh  # 该委托的资金账号
                    Weituo_ins.iloc[0, 7] = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
                    Weituo_ins.iloc[0,8]='xiaodong'
                    #Weituo_ins.iloc[0,9]=10000000.0/TotalAsset
                    Weituo_ins.iloc[0,9]=0.2
                    Weituo_ins.iloc[0,10]='no'
                    Weituo_here = pd.concat([Weituo_here, Weituo_ins],ignore_index=True)
                if bianhao in orderlist_xx:
                    Weituo_ins.iloc[0, 0] = daima  # 股票代码
                    Weituo_ins.iloc[0, 1] = bianhao  # 委托编号
                    Weituo_ins.iloc[0, 2] = cjsl  # 该委托的成交数量
                    Weituo_ins.iloc[0, 3] = cjje  # 该委托的成交金额
                    Weituo_ins.iloc[0, 4] = cjjj  # 该委托的成交均价
                    Weituo_ins.iloc[0, 5] = mmfx  # 该委托的买卖方向，通常0买1卖。
                    Weituo_ins.iloc[0, 6] = zjzh  # 该委托的资金账号
                    Weituo_ins.iloc[0, 7] = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
                    Weituo_ins.iloc[0,8]='xiaoxin'
                    #Weituo_ins.iloc[0,9]=10000000.0/TotalAsset
                    Weituo_ins.iloc[0,9]=0.1
                    Weituo_ins.iloc[0,10]='no'
                    Weituo_here = pd.concat([Weituo_here, Weituo_ins],ignore_index=True)
                    
                if bianhao in orderlist_zh:
                    Weituo_ins.iloc[0, 0] = daima  # 股票代码
                    Weituo_ins.iloc[0, 1] = bianhao  # 委托编号
                    Weituo_ins.iloc[0, 2] = cjsl  # 该委托的成交数量
                    Weituo_ins.iloc[0, 3] = cjje  # 该委托的成交金额
                    Weituo_ins.iloc[0, 4] = cjjj  # 该委托的成交均价
                    Weituo_ins.iloc[0, 5] = mmfx  # 该委托的买卖方向，通常0买1卖。
                    Weituo_ins.iloc[0, 6] = zjzh  # 该委托的资金账号
                    Weituo_ins.iloc[0, 7] = time.strftime("%Y%m%d %H:%M:%S", time.localtime())
                    Weituo_ins.iloc[0,8]='zhengshuoyu'
                    #Weituo_ins.iloc[0,9]=10000000.0/TotalAsset
                    Weituo_ins.iloc[0,9]=0.1
                    Weituo_ins.iloc[0,10]='no'
                    Weituo_here = pd.concat([Weituo_here, Weituo_ins],ignore_index=True)
                    
                    
                    
                #####################################################################

        Sendamount_all = Sendamount_all + Sendamount
        index = index + 1
        time.sleep(11)  # 稍后，登录下一个账号。
        del client

    '''
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
        SL4.columns = ['stocknum', 'QD_ZT', 'H3_HL', 'H6_ZS', 'H9_ZT', 'H6_ZT', 'H9_ZX', 'H6_GD']
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
    '''

    #交易记录生成
    Weituo_heretemp=Weituo_here.loc[:,['stockname', 'bianhao', 'amount', 'money', 'price', 'buysell', 'account', 'time','manager','managerweight','dealflag']]
    Weituo_here=Weituo_heretemp.copy()
    stock_len = len(list(Weituo_here.stockname))
    for acc_index in range(0, stock_len):
        if str(Weituo_here.iloc[acc_index, 0]).startswith(('6', '5')) and str(Weituo_here.iloc[acc_index, 0]).endswith('SH')==False:
            Weituo_here.iloc[acc_index, 0] = '.'.join([str(Weituo_here.iloc[acc_index, 0]), 'SH'])
        if str(Weituo_here.iloc[acc_index, 0]).startswith(('0', '1', '2', '3')) and str(Weituo_here.iloc[acc_index, 0]).endswith('SZ')==False:
            Weituo_here.iloc[acc_index, 0] = '.'.join([str(Weituo_here.iloc[acc_index, 0]), 'SZ'])
        if str(Weituo_here.iloc[acc_index, 6]).endswith('S')==False:
            Weituo_here.iloc[acc_index, 6] = '.'.join([str(Weituo_here.iloc[acc_index, 6]), 'S'])
    
    #####修改######################################################
    Weituo_here.to_excel(result_weituo_total[1], sheet_name="all", index=True, header=True)
    #TradingRds是生成的交易记录
    TradingRds=Weituo_here.copy()
    
    #这里要生成每一个投资经理的汇总持仓文件'./Position_xx.xlsx'
    for manager_i in range(0,managernum):
        managertemp=managerslist[manager_i]
        Positionfile=managers_posaddlist[manager_i]
        result_weituo_filename=result_weituolist[manager_i]
        result_weituoRd=TradingRds[TradingRds.manager==managertemp]
        result_weituoRd=result_weituoRd.loc[:,['stockname', 'bianhao', 'amount', 'money', 'price', 'buysell', 'time','manager','managerweight','dealflag']]
        result_weituoRd.to_excel(result_weituo_filename)
        result_weituoRd.to_excel(result_weituolist_backup[manager_i])
        #在成交记录中挑选出来投资经理的尚未处理的成交记录
        #TradingRd为投资经理的全部账户的汇总尚未处理的成交记录
        TradingRd=TradingRds[(TradingRds.manager==managertemp)&(TradingRds.dealflag=='no')]
        TradingRd=TradingRd.reset_index(drop=True)
        
        if len(TradingRd)>=0:
            
            #读入投资经理的分账户持仓文件
            Sheet_Eachaccount=pd.read_excel(Positionfile,sheetname='Sheet2')
            Sheet_Eachaccount=Sheet_Eachaccount.reset_index(drop=True)
            Sheet_Eachaccountcopy=Sheet_Eachaccount.copy()
           
            #对交易记录文件TradingRd进行循环,对于每一条的交易记录，处理完之后都要重新更新
            #持仓文件和分账户持仓文件
            for TradingRd_i in range(0,len(TradingRd)):
            
                Eachaccount_stockname=Sheet_Eachaccountcopy.iloc[2:,0]
                
                myTradingRdstockname=TradingRd.stockname.iloc[TradingRd_i]
                myTradingRdamount=TradingRd.amount.iloc[TradingRd_i]
                myTradingRdprice=TradingRd.price.iloc[TradingRd_i]
                ##############################################################
                #####接下来更新分账户的excel表格#############################
                ##############################################################
                #判断交易记录是哪个账户的。
                for inx in range(0, account_num):
                    if TradingRd.account.iloc[TradingRd_i]==str(sAccountNos[inx])+'.S':
                    #在持仓中的情况
                        if  myTradingRdstockname in Eachaccount_stockname.values:
                        
                            index=Eachaccount_stockname[Eachaccount_stockname==myTradingRdstockname].index.tolist()
                            index=index[0]
                            column=2*inx+2
                            cash_ZX=float(Sheet_Eachaccountcopy.iloc[1,column])
                            
                            
                            ZXamount=Sheet_Eachaccountcopy.iloc[index,column]
                            ZXamount_available=Sheet_Eachaccountcopy.iloc[index,column+1]
                            
                            #'0'买入，'1'卖出
                            #针对买入的情况,总持有增加，股票可用数量不变
                            if str(TradingRd.buysell.iloc[TradingRd_i])=='0':
                                ZXamount=ZXamount+myTradingRdamount
                                ZXamount_available=ZXamount_available
                                cash_ZX=cash_ZX-myTradingRdamount*myTradingRdprice
                            #针对卖出的情况，总持有减少，股票可用数量减少
                            else:
                                ZXamount=ZXamount-myTradingRdamount
                                ZXamount_available=ZXamount_available-myTradingRdamount
                                cash_ZX=cash_ZX+myTradingRdamount*myTradingRdprice    
                            #将更新后的amount记录
                   
                            Sheet_Eachaccountcopy.iloc[index,column]=ZXamount
                            Sheet_Eachaccountcopy.iloc[index,column+1]=ZXamount_available
                            Sheet_Eachaccountcopy.iloc[1,column]=cash_ZX
                           
                      
                        #不在持仓中的情况：
                        else:
                            columnsname=Sheet_Eachaccountcopy.columns.tolist()
                            columnslen=Sheet_Eachaccountcopy.columns.size
                            newbuy=DataFrame(np.zeros((1, columnslen)),columns=columnsname)
                         
                            column=2*inx+2
                            newbuy.iloc[0,0]=myTradingRdstockname
                            newbuy.iloc[0,column]=myTradingRdamount
                            cash_ZX=float(Sheet_Eachaccountcopy.iloc[1,column])
                            cash_ZX=cash_ZX-myTradingRdamount*myTradingRdprice
                            Sheet_Eachaccountcopy.iloc[1,column]=cash_ZX
                            Sheet_Eachaccountcopy=pd.concat([Sheet_Eachaccountcopy,newbuy],ignore_index=True) 
                      
                        
                        
                #生成分持仓文件的total
               
            for i in range(0,len(Sheet_Eachaccountcopy)):
                Sheet_Eachaccountcopy.iloc[i,1]=Sheet_Eachaccountcopy.iloc[i,2]+Sheet_Eachaccountcopy.iloc[i,4]
                
            print >>result_log,'------------Sheet_Eachaccount--------------'
            print >>result_log,managertemp
            print >>result_log,Sheet_Eachaccountcopy       
            
            print '------------Sheet_Eachaccount--------------'
            print managertemp
            print Sheet_Eachaccountcopy  
            
            Sheet_Eachaccountcopy.to_excel(Positionfile,sheet_name='Sheet2')
            Sheet_Eachaccountcopy.to_excel(managers_posaddlist_backup[manager_i],sheet_name='Sheet2')
            ##################################################################
            ##################################################################
                
    #处理好之后更新交易记录的处理标记
    Weituo_here.dealflag=Weituo_here.dealflag.map({'no':'yes','yes':'yes'})
    Weituo_here.to_excel(result_weituo_total[0], sheet_name="all", index=True, header=True)
    ######修改完成########################################################
    

    if Sendamount_all == 0 and isok == 1:
        print "timeindex =    " + str(timeindex)
        #print >> f, "timeindex =    " + str(timeindex)
        #print >> f, time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        print time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        print "All accounts have been adjusted the ratio defined in result_send.csv. Finished!"
        #print >> f, "All accounts have been adjusted the ratio defined in result_send.csv. Finished!"
        break
    else:
        print "All accounts have run once, wait for several minutes for the next run: "
        #print >> f, "Every account have run once, wait for several minutes for the next run: "
        print "timeindex =    " + str(timeindex)
        #print >> f, "timeindex =    " + str(timeindex)
        print "Begin wait X minutes... Please see result_log_num.txt to know detail!"
        #print >> f, "Begin wait X minutes... Please see result_log_num.txt to know detail!"
        #print >> f, time.strftime("%Y%m%d %H:%M:%S", time.localtime())
        print time.strftime("%Y%m%d %H:%M:%S", time.localtime())


    timeindex = timeindex + 1
    result_log.close()
    TradeX.CloseTdx()
    if account_num == 6:
        time.sleep(sleeptime)
    if account_num == 2:
        time.sleep(15)
    endtime = datetime.datetime.now()
    print '-----program runtime is: ----------'
    print (endtime - starttime).seconds
    
    # 账户少的程序组，等一等账户多的程序组。