import cx_Oracle as cxo
import pandas as pd
import numpy as np
from statsmodels.formula.api import ols


import matplotlib.pyplot as plt
import matplotlib as mpl
import locale
import matplotlib.ticker as ticker
import matplotlib.dates as mdates

st_dt = '20180101'
ed_dt = '20180630'

## 쿼리를 읽어오기 위한 클래스
class query:

    def __init__(self):

        self.engine = cxo.connect(user='uffdba', password='venus2006', dsn='funddb')

    def read_sql(self, query):

        dt = pd.read_sql(con = self.engine,
                         sql = query, index_col='STD_DT')

        dt.index = pd.to_datetime(dt.index)

        return dt

    def rf_read_sql(self, rf_query):

        dt = self.read_sql(rf_query)

        return ((dt.div(100).sub(-1)) ** (1/365)).loc[st_dt:]

## 해당 개별 쿼리 내에서 상위 자산군 / 임의 자산군으로 통합하기 위한 클래스
class data_aggr:

    def __init__(self, data_list):

        self.data_list = data_list
        self.date_range = pd.date_range(st_dt, ed_dt)
        self.columns = ['전일자평가금액', '평가금액',
                        '장부금액', '매도', '매수', '별도',
                        '기말금액', '기초금액', '수익률']

    def aggr_dt(self, ast_type):

        dt = pd.DataFrame(index = self.date_range,
                          columns= self.columns,
                     data = np.zeros((len(self.date_range),
                                      len(self.columns))))

        for data in self.data_list:
            dt = (dt + data.drop('자산구분', axis=1)).fillna(0)

        dt['자산구분'] = ast_type
        dt['수익률'] = dt['기말금액'] / dt['기초금액']

        return dt

## 수익률, 기초, 기말 자산 등을 불러오기 위한 클래스
class feat_extraction:

    def __init__(self, data_list):
        self.data = pd.concat(data_list, axis = 0)

    def rt(self):
        return self.data[['자산구분', '수익률']].pivot(columns='자산구분', values = '수익률')

    def book_val(self):
        return self.data[['자산구분', '장부금액']].pivot(columns='자산구분', values = '장부금액')

    def mkt_val(self):
        return self.data[['자산구분', '평가금액']].pivot(columns='자산구분', values = '평가금액')

    def b_mkt_val(self):
        return self.data[['자산구분', '전일자평가금액']].pivot(columns='자산구분', values = '전일자평가금액')

    def buy(self):
        return self.data[['자산구분', '매수']].pivot(columns='자산구분', values = '매수')

    def sell(self):
        return self.data[['자산구분', '매도']].pivot(columns='자산구분', values = '매도')

    def other(self):
        return self.data[['자산구분', '별도']].pivot(columns='자산구분', values = '별도')

    def net_buy(self):
        return self.buy().sub(self.sell())

    def bf_ast_val(self):
        return self.data[['자산구분', '기초금액']].pivot(columns='자산구분', values = '기초금액')

    def af_ast_val(self):
        return self.data[['자산구분', '기말금액']].pivot(columns='자산구분', values = '기말금액')

    def bf_ast_rto(self, st_cols = '기금전체'):

        return self.bf_ast_val().div(self.bf_ast_val()[st_cols], axis = 0).drop(st_cols, axis = 1)

    def book_val_rto(self, st_cols = '기금전체'):

        return self.book_val().div(self.book_val()[st_cols], axis = 0).drop(st_cols, axis = 1)

## 수익률 등을 활용한 기초지표를 산출하기 위한 클래스
class rt_stat:

    # 주의: BM데이터의 포맷과 RETURN 데이터의 포맷은 똑같아야함. 안그러면 망 ㅎ

    def __init__(self, ret, rf, bm):

        self.dt_list = ret.index
        self.rt = ret
        self.rf = rf
        self.bm = bm

    def avg_ret(self, Freq='A'):

        ans = {'rt': self.rt.resample(Freq).mean().sub(1).div(1 / 365),
               'rf': self.rf.resample(Freq).mean().sub(1).div(1 / 365),
               'bm': self.bm.resample(Freq).mean().sub(1).div(1 / 365)}

        return ans

    def prod_ret(self, Freq='A'):

        ans = {'rt': self.rt.resample(Freq).prod().sub(1),
               'rf': self.rf.resample(Freq).prod().sub(1),
               'bm': self.bm.resample(Freq).prod().sub(1)}

        return ans

    def cum_rt(self):

        ans = {'rt': self.rt.cumprod(),
               'rf': self.rf.cumprod(),
               'bm': self.bm.cumprod()}

        return ans

    def ret_vol(self, Freq='A'):

        def s_std(x):
            return np.std(x, ddof=1) * np.sqrt(365)

        ans = {'rt': self.rt.resample(Freq).apply(s_std),
               'bm': self.bm.resample(Freq).apply(s_std)}

        return ans

    def trek_error(self, Freq='A'):

        def s_std(x):
            return np.std(x, ddof=1) * np.sqrt(365)

        ans = (self.rt.sub(self.bm)) \
            .resample(Freq) \
            .apply(s_std)

        return ans

    def sharpe_rto(self, Freq='A'):

        a_rt = self.avg_ret(Freq)
        a_vol = self.ret_vol(Freq)

        ## 운용수익률 샤프
        rt_shp = a_rt['rt'].sub(a_rt['rf']['RF'], axis=0)
        rt_shp.where(rt_shp < 0, rt_shp.div(a_vol['rt']), inplace=True)
        rt_shp.where(rt_shp >= 0, rt_shp.div(1/a_vol['rt']), inplace=True)

        ## BM수익률 샤프

        bm_shp = a_rt['bm'].sub(a_rt['rf']['RF'], axis=0)
        bm_shp.where(bm_shp < 0, bm_shp.div(a_vol['bm']), inplace=True)
        bm_shp.where(bm_shp >= 0, bm_shp.div(1 / a_vol['bm']), inplace=True)

        ans = {'rt_shp': rt_shp,
               'bm_shp': bm_shp}

        return ans

    def inf_rto(self, Freq='A'):

        a_rt = self.avg_ret(Freq)
        a_tr = self.trek_error(Freq)

        rt_tre = (a_rt['rt'].sub(a_rt['bm'])).div(a_tr)

        return rt_tre

    def alp_beta(self):

        cols = self.rt.columns
        ans_dt = pd.DataFrame(index=cols,
                          columns=['beta', 'alpha'],
                          data= np.zeros((len(cols), 2)))

        for col in cols:
            rt_dt = self.rt[col].sub(self.rf['RF']).dropna().rename('y')
            bm_dt = self.bm[col].sub(self.rf['RF']).loc[rt_dt.index].rename('x')

            data_1 = pd.concat([bm_dt, rt_dt], axis=1)
            result = ols('y~x', data_1).fit()
            ans_dt.loc[col, 'beta'] = result.params['x']
            ans_dt.loc[col, 'alpha'] = result.params['Intercept']

        return ans_dt

    def tryn_rto(self):

        return self.avg_ret()['rt'].sub(self.avg_ret()['rf']['RF'], axis=0).div(self.alp_beta()['beta'])

## 그래프를 산출하기 위한 옵션 설정 등울 클래스
class plot_lib:

    plt.rcParams['font.family'] = 'NanumGothic'
    plt.rcParams['font.size'] = 15
    plt.rcParams['axes.unicode_minus'] = False

    def __init__(self, data):
        self.fig, self.ax = plt.subplots()
        self.fig.set_size_inches((11, 8))
        self.data = data
        self.cols = self.data.columns
        self.idx = self.data.index

    def ticks_label_opt(self):
        ## Ticks / xlabel / ylabel의 공통옵션

        self.ax.minorticks_off()
        self.ax.set_xlabel('')
        self.ax.set_ylabel('')
        self.ax.legend(loc='best', ncol=3)

        return self.ax

    def remove_spine(self):
        for spn in ['top', 'right']:
            self.ax.spines[spn].set_color('None')

        return self.ax

    def PercFormatter(self):
        return self.ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:.2%}'.format(y)))

    def MoneyFormatter(self):
        return self.ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:,}'.format(y)))

    def right_PercFormatter(self):
        return self.ax.right_ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:.2%}'.format(y)))

    def right_MoneyFormatter(self):
        return self.ax.right_ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:,}'.format(y)))

    def tick_rote(self):
        ## Only for the string Variable(Not DateIndex Obeject)
        return self.ax.set_xticklabels(labels=self.ax.get_xticklabels(), rotation=0)

    def date_x_tick(self):
        ticklbls = pd.date_range(self.idx[0], self.idx[-1], freq='MS')
        ticklbls = [item.strftime('%y.%B') for item in ticklbls]
        self.ax.xaxis.set_major_locator(mdates.MonthLocator())
        self.ax.xaxis.set_major_formatter(ticker.FixedFormatter(ticklbls))

        return self.ax

    def x_ticks_disc(self):
        ## 두 개의 x-axis를 쓸 때 사용할 것.(특히 Bar + Line 조합)
        ticks_n = self.data.shape[0]
        lt_tick = [i for i in range(0, ticks_n)]
        self.ax.set_xticks(lt_tick)
        self.ax.set_xticklabels(self.idx)
        self.ax.set_xlim(xmin=-0.5, xmax=ticks_n - 0.5)

        return self.ax

    def y_annotation(self, tpe='per'):

        ## Type = 'per' - 퍼센트 / 'cur' - 통화

        for p in self.ax.patches:
            if tpe == 'per':
                self.ax.annotate('{:.2%}'.format(p.get_height()), (p.get_x() * 1.005, p.get_height() * 1.05))
            elif tpe == 'cur':
                self.ax.annotate('{:,}'.format(p.get_height()), (p.get_x() * 1.005, p.get_height() * 1.05))

        return self.ax

    def t_series_line_plot(self, plot_type='rt'):

        self.data.plot(ax=self.ax, legend=True, lw=3)
        self.ticks_label_opt()
        self.date_x_tick()
        self.tick_rote()

        if plot_type == 'rt':
            self.PercFormatter()
        elif plot_type == 'cur':
            self.MoneyFormatter()

        return self.fig, self.ax

    def t_series_area_plot(self, ytype='rt', sec_ytype='rt'):

        cols = self.data.columns

        self.data[cols[:2]](ax=self.ax, legend=True, lw=3)

        if ytype == 'rt':
            self.PercFormatter()
        elif ytype == 'cur':
            self.MoneyFormatter()
        else:
            self.PercFormatter()

        self.data[cols[2]].plot(kind='area', legend=True
                                , stacked=False, secondary_y=True)
        if sec_ytype == 'rt':
            self.right_PercFormatter()
        elif sec_ytype == 'cur':
            self.right_MoneyFormatter()
        else:
            self.right_PercFormatter()

        self.ticks_label_opt()
        self.date_x_tick()
        self.tick_rote()

        return self.fig, self.ax

    def line_bar_plot(self, line_col, bar_col, ytype='rt', sec_ytype='rt'):

        self.data[line_col](ax=self.ax, legend=True, lw=3)

        if ytype == 'rt':
            self.PercFormatter()
        elif ytype == 'cur':
            self.MoneyFormatter()
        else:
            self.PercFormatter()

        self.data[bar_col].plot(kind='bar', legend=True
                                , secondary_y=True)
        if sec_ytype == 'rt':
            self.right_PercFormatter()
        elif sec_ytype == 'cur':
            self.right_MoneyFormatter()
        else:
            self.right_PercFormatter()

        self.ticks_label_opt()
        self.x_ticks_disc()
        self.tick_rote()

        return self.fig, self.ax



# 자산군 쿼리

sql_dom_bd_j = """
WITH FT AS (
SELECT A.STD_DT
       , A.평가금액
       , A.장부금액
       , NVL(B."채권원리금상환", 0) AS 채권원리금상환
       , NVL(B."주식매도", 0) AS 주식매도
       , NVL(B."채권매도", 0) AS 채권매도                   
       , NVL(B."채권매수", 0) AS 채권매수          
       , NVL(B."채권수요예측", 0) AS 채권수요예측          
       , NVL(B."채권이자수령", 0) AS 채권이자수령
       , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
FROM 
(
SELECT     STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('111010', '111020')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT
) A,
(
           SELECT    A.STD_DT,
                      SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
           FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
           WHERE     A.FUND_CD IN ('111010', '111020')
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
           AND       A.STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  A.STD_DT
           ORDER BY  A.STD_DT           
) B
WHERE 1=1
AND   A.STD_DT = B.STD_DT(+)
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '국내채권직접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""

## 만기별 비중

sql_dom_bd_sxm = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT < '20181230' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_sxmoy = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20181231' AND '20190629' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월~1년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_oytw = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20190630' AND '20200629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '1년~2년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_twth = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20200630' AND '20210629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '2년~3년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_thfv = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20210630' AND '20230629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '3년~5년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fvty = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20230630' AND '20280629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '5년~10년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tytwy = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20280630' AND '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '10년~20년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_twy = """
WITH FT AS (
SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT > '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '20년이상' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""

sql_dom_bd_tr = """
WITH FT AS (
SELECT A.STD_DT
       , A.평가금액
       , A.장부금액
       , NVL(B."채권원리금상환", 0) AS 채권원리금상환
       , NVL(B."주식매도", 0) AS 주식매도
       , NVL(B."채권매도", 0) AS 채권매도                   
       , NVL(B."채권매수", 0) AS 채권매수          
       , NVL(B."채권수요예측", 0) AS 채권수요예측          
       , NVL(B."채권이자수령", 0) AS 채권이자수령
       , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
FROM 
(
SELECT     STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('111010', '111020')
                     AND CLAS_10_CD IN ('BN110')                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT
) A,
(
           SELECT    A.STD_DT,
                      SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
           FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
           WHERE     A.FUND_CD IN ('111010', '111020')
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
           AND       A.CLAS_10_CD IN ('BN110')
           AND       A.STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  A.STD_DT
           ORDER BY  A.STD_DT           
) B
WHERE 1=1
AND   A.STD_DT = B.STD_DT(+)
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '국내채권직접_국고' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 국고

sql_dom_bd_tr_sxm = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN110')                             
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.CLAS_10_CD IN ('BN110')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT < '20181230' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tr_sxmoy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN110')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN110')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20181231' AND '20190629' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월~1년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tr_oytw = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN110')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN110')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20190630' AND '20200629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '1년~2년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tr_twth = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN110')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN110')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20200630' AND '20210629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '2년~3년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tr_thfv = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN110')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN110')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20210630' AND '20230629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '3년~5년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tr_fvty = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN110')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN110')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20230630' AND '20280629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '5년~10년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tr_tytwy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN110')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN110')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20280630' AND '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '10년~20년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0  AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_tr_twy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND       CLAS_10_CD IN ('BN110')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B                   
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN110')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT > '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '20년이상' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""


sql_dom_bd_fnb = """
WITH FT AS (
SELECT A.STD_DT
       , A.평가금액
       , A.장부금액
       , NVL(B."채권원리금상환", 0) AS 채권원리금상환
       , NVL(B."주식매도", 0) AS 주식매도
       , NVL(B."채권매도", 0) AS 채권매도                   
       , NVL(B."채권매수", 0) AS 채권매수          
       , NVL(B."채권수요예측", 0) AS 채권수요예측          
       , NVL(B."채권이자수령", 0) AS 채권이자수령
       , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
FROM 
(
SELECT     STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('111010', '111020')
                     AND CLAS_10_CD IN ('BN120')                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT
) A,
(
           SELECT    A.STD_DT,
                      SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
           FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
           WHERE     A.FUND_CD IN ('111010', '111020')
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
           AND       A.CLAS_10_CD IN ('BN120')
           AND       A.STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  A.STD_DT
           ORDER BY  A.STD_DT           
) B
WHERE 1=1
AND   A.STD_DT = B.STD_DT(+)
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '국내채권직접_금융' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 금융

sql_dom_bd_fnb_sxm = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN120')                             
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.CLAS_10_CD IN ('BN120')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT < '20181230' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fnb_sxmoy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN120')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN120')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20181231' AND '20190629' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월~1년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fnb_oytw = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN120')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN120')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20190630' AND '20200629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '1년~2년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fnb_twth = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN120')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN120')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20200630' AND '20210629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '2년~3년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fnb_thfv = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN120')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN120')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20210630' AND '20230629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '3년~5년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fnb_fvty = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN120')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN120')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20230630' AND '20280629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '5년~10년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fnb_tytwy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN120')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN120')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20280630' AND '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '10년~20년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0  AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_fnb_twy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND       CLAS_10_CD IN ('BN120')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B                   
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN120')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT > '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '20년이상' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""

sql_dom_bd_gse = """
WITH FT AS (
SELECT A.STD_DT
       , A.평가금액
       , A.장부금액
       , NVL(B."채권원리금상환", 0) AS 채권원리금상환
       , NVL(B."주식매도", 0) AS 주식매도
       , NVL(B."채권매도", 0) AS 채권매도                   
       , NVL(B."채권매수", 0) AS 채권매수          
       , NVL(B."채권수요예측", 0) AS 채권수요예측          
       , NVL(B."채권이자수령", 0) AS 채권이자수령
       , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
FROM 
(
SELECT     STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('111010', '111020')
                     AND CLAS_10_CD IN ('BN130')                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT
) A,
(
           SELECT    A.STD_DT,
                      SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
           FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
           WHERE     A.FUND_CD IN ('111010', '111020')
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
           AND       A.CLAS_10_CD IN ('BN130')
           AND       A.STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  A.STD_DT
           ORDER BY  A.STD_DT           
) B
WHERE 1=1
AND   A.STD_DT = B.STD_DT(+)
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '국내채권직접_특수' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 특수

sql_dom_bd_gse_sxm = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN130')                             
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.CLAS_10_CD IN ('BN130')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT < '20181230' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_gse_sxmoy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN130')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN130')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20181231' AND '20190629' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월~1년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_gse_oytw = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN130')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN130')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20190630' AND '20200629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '1년~2년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_gse_twth = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN130')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN130')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20200630' AND '20210629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '2년~3년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_gse_thfv = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN130')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN130')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20210630' AND '20230629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '3년~5년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_gse_fvty = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN130')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN130')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20230630' AND '20280629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '5년~10년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_gse_tytwy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN130')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN130')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20280630' AND '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '10년~20년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0  AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_gse_twy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND       CLAS_10_CD IN ('BN130')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B                   
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN130')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT > '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '20년이상' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""

sql_dom_bd_corp = """
WITH FT AS (
SELECT A.STD_DT
       , A.평가금액
       , A.장부금액
       , NVL(B."채권원리금상환", 0) AS 채권원리금상환
       , NVL(B."주식매도", 0) AS 주식매도
       , NVL(B."채권매도", 0) AS 채권매도                   
       , NVL(B."채권매수", 0) AS 채권매수          
       , NVL(B."채권수요예측", 0) AS 채권수요예측          
       , NVL(B."채권이자수령", 0) AS 채권이자수령
       , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
FROM 
(
SELECT     STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('111010', '111020')
                     AND CLAS_10_CD IN ('BN140', 'ST150')                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT
) A,
(
           SELECT    A.STD_DT,
                      SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                     ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
           FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
           WHERE     A.FUND_CD IN ('111010', '111020')
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
           AND       A.CLAS_10_CD IN ('BN140', 'ST150')
           AND       A.STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  A.STD_DT
           ORDER BY  A.STD_DT           
) B
WHERE 1=1
AND   A.STD_DT = B.STD_DT(+)
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '국내채권직접_회사' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 회사

sql_dom_bd_corp_sxm = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN140', 'ST150')                             
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       A.CLAS_10_CD IN ('BN140', 'ST150')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT < '20181230' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_corp_sxmoy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN140', 'ST150')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN140', 'ST150')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20181231' AND '20190629' -- 변경
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '6개월~1년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_corp_oytw = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN140', 'ST150')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN140', 'ST150')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20190630' AND '20200629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '1년~2년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_corp_twth = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN140', 'ST150')                   
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN140', 'ST150')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20200630' AND '20210629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '2년~3년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_corp_thfv = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN140', 'ST150')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND CLAS_10_CD IN ('BN140', 'ST150')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20210630' AND '20230629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '3년~5년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_corp_fvty = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN140', 'ST150')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN140', 'ST150')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20230630' AND '20280629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '5년~10년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_corp_tytwy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND CLAS_10_CD IN ('BN140', 'ST150')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN140', 'ST150')                   
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT BETWEEN '20280630' AND '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '10년~20년' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0  AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_bd_corp_twy = """
WITH FT AS (SELECT A.STD_DT 
       , SUM(A.평가금액) 평가금액
       , SUM(A.장부금액) 장부금액       
       , SUM(A.채권원리금상환) 채권원리금상환              
       , SUM(A.주식매도) 주식매도       
       , SUM(A.채권매도) 채권매도           
       , SUM(A.채권매수) 채권매수       
       , SUM(A.채권수요예측) 채권수요예측              
       , SUM(A.채권이자수령) 채권이자수령       
       , SUM(A.주식매도대금수령) 주식매도대금수령       
FROM (
        SELECT A.STD_DT
               , A.ITMS_CD
               , A.평가금액
               , A.장부금액
               , NVL(B."채권원리금상환", 0) AS 채권원리금상환
               , NVL(B."주식매도", 0) AS 주식매도
               , NVL(B."채권매도", 0) AS 채권매도                   
               , NVL(B."채권매수", 0) AS 채권매수          
               , NVL(B."채권수요예측", 0) AS 채권수요예측          
               , NVL(B."채권이자수령", 0) AS 채권이자수령
               , NVL(B."주식매도대금수령", 0) AS 주식매도대금수령
        FROM 
        (
        SELECT     STD_DT
                             , ITMS_CD
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                             , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                   FROM      CX_TP_ACNT_BY_PSBD
                   WHERE     1=1 
                             AND       CLAS_10_CD IN ('BN140', 'ST150')
                             AND FUND_CD IN ('111010', '111020')
                             AND STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
        ) A,
        (
                   SELECT    A.STD_DT
                             ,ITMS_CD
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT END) AS "채권원리금상환"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1110' THEN A.STTL_AMT END) AS "채권매도"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1100' THEN A.STTL_AMT END) AS "채권매수"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B1350' THEN A.STTL_AMT END) AS "채권수요예측"                     
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'B4200' THEN A.STTL_AMT END) AS "채권이자수령"
                             ,SUM(CASE WHEN A.TRSC_TP_CD = 'S3020' THEN A.STTL_AMT END) AS "주식매도대금수령"                     
                   FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B                   
                   WHERE     A.FUND_CD IN ('111010', '111020')
                   AND       CLAS_10_CD IN ('BN140', 'ST150')
                   AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                   AND       A.TRSC_TP_CD IN ('B4310', 'S1110','B1110','B1100','B1350','B4200', 'S3020')
                   AND       A.STD_DT BETWEEN '20171231' AND '20180630'
                   GROUP BY  STD_DT, ITMS_CD
                   ORDER BY  A.STD_DT           
        ) B,
        (
            SELECT ITMS_CD, EXPR_DT
            FROM CX_TP_ASET_BAS_INF
        ) C
            WHERE 1=1
                  AND A.STD_DT = B.STD_DT(+)
                  AND A.ITMS_CD = B.ITMS_CD(+)
                  AND A.ITMS_CD = C.ITMS_CD(+)
                  AND C.EXPR_DT > '20380629'
                  ORDER BY A.STD_DT
) A
GROUP BY A.STD_DT
ORDER BY A.STD_DT), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+주식매도+채권매도 매도 
       , 채권매수+채권수요예측 매수
       , 채권이자수령+주식매도대금수령 별도  
FROM FT
) SELECT STD_DT, '20년이상' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
    FROM SND
    WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""





## 해외주식간접: 액티브/패시브
## 국내간접대체: 등등

sql_dom_fn = """
WITH FT AS (
SELECT    A.*,
          NVL("장기금융상품상환", 0) AS 장기금융상품상환,
          NVL("장기금융상품매수", 0) AS 장기금융상품매수
FROM      ( --금융상품 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND FUND_CD IN ('314010', '315010')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --금융상품 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'M4600' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "장기금융상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'M1300' THEN A.STTL_AMT END) AS "장기금융상품매수"
           FROM      CX_TP_ACNT_BY_TRBD A
           WHERE     1=1
                     AND A.FUND_CD IN ('314010', '315010')
                     AND A.TRSC_TP_CD IN ('M4600', 'M1300')
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 장기금융상품상환 매도 
       , 장기금융상품매수 매수
       , 0 별도  
FROM FT
) SELECT STD_DT,'금융상품' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '2018630'"""

sql_ov_bd_j = """
WITH FT AS (
SELECT A.*,
     NVL ("해외FORWARD결산", 0) 해외FORWARD결산 ,         
     NVL ("채권원리금상환", 0) 채권원리금상환
FROM (--일별 평가금액
      SELECT   STD_DT,
               SUM (CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
               SUM (CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
          FROM CX_TP_ACNT_BY_PSBD
          WHERE     FUND_CD IN ('111110', '111120')
                    AND STD_DT BETWEEN '20171231' AND '20180630'
      GROUP BY STD_DT) A,
     (--일별 거래금액
      SELECT   A.STD_DT,
               SUM (CASE WHEN A.TRSC_TP_CD = 'B4310' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "채권원리금상환",
               SUM (CASE WHEN A.TRSC_TP_CD = 'F9100' THEN A.STTL_AMT END) AS "해외FORWARD결산"
             FROM CX_TP_ACNT_BY_TRBD A
--                  , CX_TP_BO_TRSC_TP B
             WHERE     A.FUND_CD IN ('111110', '111120')
                       AND A.STD_DT BETWEEN '20171231' AND '2018630'
--                       AND A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                       AND A.TRSC_TP_CD IN ('F9100','B4310')
      GROUP BY A.STD_DT) B
WHERE A.STD_DT = B.STD_DT(+)
ORDER BY A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 채권원리금상환+해외FORWARD결산 매도 
       , 0 매수
       , 0 별도  
FROM FT
) SELECT STD_DT, '해외채권직접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'"""
sql_ov_bd_g = """
WITH FT AS (
SELECT    A.*,
          NVL(B."간접상품추가투자", 0) AS 간접상품추가투자,
          NVL(B."간접상품매수", 0) AS 간접상품매수,
          NVL(B."간접상품재투자", 0) AS 간접상품재투자
FROM      ( --국내채권직접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('411110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권직접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자"
           FROM      CX_TP_ACNT_BY_TRBD A
           WHERE     1=1
                     AND A.FUND_CD IN ('411110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
                     AND A.TRSC_TP_CD IN ('O4400','O1100','O4300')
           GROUP BY  A.STD_DT) B
WHERE 1=1
        AND     A.STD_DT = B.STD_DT(+)
        AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 0 매도 
       , 간접상품추가투자 매수
       , 간접상품재투자 별도  
FROM FT
) SELECT STD_DT, '해외채권간접' 자산구분, 전일자평가금액, 평가금액
        , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'"""

sql_dom_stk_j = """
WITH FT AS (
SELECT    A.*,
          NVL(B."주식매도", 0) AS 주식매도,
--          NVL(B."주식편출", 0) AS 주식편출,
          NVL(B."단기매매주식매수", 0) AS 단기매매주식매수,
--          NVL(B."주식편입", 0) AS 주식편입,
--          NVL(B."주식낙찰", 0) AS 주식낙찰,
--          NVL(B."기업합병단주대금수령", 0) AS 기업합병단주대금수령,
          NVL(B."유무상단주대금수령", 0) AS 유무상단주대금수령,
--          NVL(B."현금배당금수령", 0) AS 현금배당금수령,
          NVL(B."배당단주대금수령", 0) AS 배당단주대금수령,          
          NVL(B."현금배당금확정", 0) AS 현금배당금확정          
FROM      ( -- 국내주식직접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND FUND_CD IN ('212000', '213000')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내주식직접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'S1110' THEN A.STTL_AMT END) AS "주식매도",
--                     SUM(CASE WHEN A.TRSC_TP_CD = 'S1210' THEN A.STTL_AMT END) AS "주식편출",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'S1100' THEN A.STTL_AMT END) AS "단기매매주식매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'S1200' THEN A.STTL_AMT END) AS "주식편입",
--                     SUM(CASE WHEN A.TRSC_TP_CD = 'S1310' THEN A.STTL_AMT END) AS "주식낙찰",
--                     SUM(CASE WHEN A.TRSC_TP_CD = 'S4340' THEN A.STTL_AMT END) AS "기업합병단주대금수령",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'S4330' THEN A.STTL_AMT END) AS "유무상단주대금수령",
--                     SUM(CASE WHEN A.TRSC_TP_CD = 'S4320' THEN A.STTL_AMT END) AS "현금배당금수령",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'S4310' THEN A.STTL_AMT END) AS "현금배당금확정",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'S4380' THEN A.STTL_AMT END) AS "배당단주대금수령"                     
           FROM      CX_TP_ACNT_BY_TRBD A
--                         , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('212000', '213000')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('S1110','S1210','S1100','S1200','S1310','S4340','S4330','X1500','S4310','S4320','S4380')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 주식매도 매도 
       , 단기매매주식매수 매수
       , 유무상단주대금수령 + 배당단주대금수령 + 현금배당금확정 별도  
FROM FT
) SELECT STD_DT, '국내주식직접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_stk_g = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""

## 국내주식간접:
sql_dom_stk_g = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_stk_g_grw = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND CLAS_11_CD = 'OS221'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND A.CLAS_11_CD = 'OS221'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접_성장' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" # 성장형
sql_dom_stk_g_idx = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND CLAS_11_CD = 'OS222'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND A.CLAS_11_CD = 'OS222'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접_인덱스' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" # 인덱스형
sql_dom_stk_g_smb = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND CLAS_11_CD = 'OS223'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND A.CLAS_11_CD = 'OS223'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접_중소형' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" # 중소형
sql_dom_stk_g_esg = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND CLAS_11_CD = 'OS224'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND A.CLAS_11_CD = 'OS224'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접_사회책임' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" # 사회책임형
sql_dom_stk_g_div = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND CLAS_11_CD = 'OS225'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND A.CLAS_11_CD = 'OS225'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접_배당' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" # 배당형
sql_dom_stk_g_val = """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND CLAS_11_CD = 'OS226'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND A.CLAS_11_CD = 'OS226'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접_가치' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" # 가치형
sql_dom_stk_g_acq =  """
WITH FT AS (
SELECT    A.*,
              NVL(B."간접상품현물이체", 0) AS "간접상품현물이체",
              NVL(B."간접상품상환", 0) AS "간접상품상환",
              NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",          
              NVL(B."간접상품현물인수", 0) AS "간접상품현물인수",
              NVL(B."간접상품매수", 0) AS "간접상품매수",
              NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
              NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"
FROM      ( -- 국내주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END)  AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('412010')
                     AND CLAS_11_CD = 'OS227'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --국내채권간접 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1410' THEN A.STTL_AMT END) AS "간접상품현물이체",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1400' THEN A.STTL_AMT END) AS "간접상품현물인수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                       , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412010')
                     AND A.CLAS_11_CD = 'OS227'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'
--               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1410', 'O1110', 'O4400', 'O1400','O1100','O3200','O4200')
           GROUP BY  A.STD_DT) B
WHERE
1=1
AND     A.STD_DT = B.STD_DT(+)
AND     A.STD_DT BETWEEN '20171231' AND '20180630'
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품현물이체 + 간접상품상환 매도 
       , 간접상품추가투자 + 간접상품현물인수 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내주식간접_액티브퀀트' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" # 액티브퀀형


sql_ov_stk_g = """WITH FT AS (
SELECT    A.*,
--          NVL(B."간접상품상환", 0) AS "간접상품상환",
          NVL(B."간접상품매수", 0) AS "간접상품매수",          
          NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",
          NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
          NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"          
FROM      ( -- 해외주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND STD_DT BETWEEN '20171231' AND '20180630'
                     AND FUND_CD IN ('412110')
           GROUP BY  STD_DT) A,
          ( --해외주식간직접 일별 거래금액
           SELECT    A.STD_DT,
--                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT END) AS "간접상품현금분배금수령",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자"
           FROM      CX_TP_ACNT_BY_TRBD A,
                     CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'           
                     AND A.TRSC_TP_CD = B.TRSC_TP_CD
                     AND A.TRSC_TP_CD IN ('O1100', 'O4200','O4300','O4400')
           GROUP BY  A.STD_DT) B
WHERE 1=1
      AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 0 매도 
       , 간접상품추가투자 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외주식간접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'"""
sql_ov_stk_g_act = """WITH FT AS (
SELECT    A.*,
--          NVL(B."간접상품상환", 0) AS "간접상품상환",
          NVL(B."간접상품매수", 0) AS "간접상품매수",          
          NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",
          NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
          NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"          
FROM      ( -- 해외주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND STD_DT BETWEEN '20171231' AND '20180630'
                     AND CLAS_28_CD = 'OS323'                     
                     AND FUND_CD IN ('412110')
           GROUP BY  STD_DT) A,
          ( --해외주식간직접 일별 거래금액
           SELECT    A.STD_DT,
--                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT END) AS "간접상품현금분배금수령",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자"
           FROM      CX_TP_ACNT_BY_TRBD A,
                     CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412110')
                     AND A.CLAS_28_CD = 'OS323'                     
                     AND STD_DT BETWEEN '20171231' AND '20180630'           
                     AND A.TRSC_TP_CD = B.TRSC_TP_CD
                     AND A.TRSC_TP_CD IN ('O1100', 'O4200','O4300','O4400')
           GROUP BY  A.STD_DT) B
WHERE 1=1
      AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 0 매도 
       , 간접상품추가투자 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외주식간접_액티브' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'"""
sql_ov_stk_g_psv = """WITH FT AS (
SELECT    A.*,
--          NVL(B."간접상품상환", 0) AS "간접상품상환",
          NVL(B."간접상품매수", 0) AS "간접상품매수",          
          NVL(B."간접상품추가투자", 0) AS "간접상품추가투자",
          NVL(B."간접상품재투자", 0) AS "간접상품재투자",          
          NVL(B."간접상품현금분배금수령", 0) AS "간접상품현금분배금수령"          
FROM      ( -- 해외주식간접 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND STD_DT BETWEEN '20171231' AND '20180630'
                     AND CLAS_28_CD = 'OS324'                     
                     AND FUND_CD IN ('412110')
           GROUP BY  STD_DT) A,
          ( --해외주식간직접 일별 거래금액
           SELECT    A.STD_DT,
--                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT END) AS "간접상품현금분배금수령",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자"
           FROM      CX_TP_ACNT_BY_TRBD A,
                     CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND A.FUND_CD IN ('412110')
                     AND A.CLAS_28_CD = 'OS324'
                     AND STD_DT BETWEEN '20171231' AND '20180630'           
                     AND A.TRSC_TP_CD = B.TRSC_TP_CD
                     AND A.TRSC_TP_CD IN ('O1100', 'O4200','O4300','O4400')
           GROUP BY  A.STD_DT) B
WHERE 1=1
      AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 0 매도 
       , 간접상품추가투자 + 간접상품매수 매수
       , 간접상품재투자 + 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외주식간접_패시브' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'"""

sql_csh = """
WITH FT AS (
SELECT    A.*,                  
          NVL("간접상품상환", 0) AS 간접상품상환,
          NVL("단기예치금매도", 0) AS 단기예치금매도,
          NVL("단기예치금매수", 0) AS 단기예치금매수,          
          NVL("간접상품매수", 0) AS 간접상품매수,
          NVL("간접상품재투자", 0) AS 간접상품재투자,
          NVL("예치금이자수령", 0) AS 예치금이자수령,
          NVL("보통예금이자수령", 0) AS 보통예금이자수령
FROM      ( --현금성 일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND STD_DT BETWEEN '20171231' AND '20180630'
                     AND FUND_CD IN ('313010')
           GROUP BY  STD_DT) A,
          ( --현금성 일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'M1210' THEN A.STTL_AMT END) AS "단기예치금매도",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'M1200' THEN A.STTL_AMT END) AS "단기예치금매수",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",                     
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4300' THEN A.ADPY_CRP_TAX END) AS "간접상품재투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4600' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "예치금이자수령",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'C3100' THEN A.STTL_AMT+A.ADPY_CRP_TAX END) AS "보통예금이자수령"                     
           FROM      CX_TP_ACNT_BY_TRBD A
--                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1 
                     AND A.FUND_CD IN ('313010')
                     AND STD_DT BETWEEN '20161231' AND '20180630'           
--           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1110', 'M1210','M1200','O1100','O4300','O4600','C3100')
           GROUP BY  A.STD_DT) B
WHERE 1=1
      AND A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 + 단기예치금매도 매도 
       , 단기예치금매수 + 간접상품매수 매수
       , 간접상품재투자 + 예치금이자수령 + 보통예금이자수령 별도  
FROM FT
) SELECT STD_DT, '현금성' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'"""

## 국내대체직접

sql_dom_ai_j = """
WITH FT AS (
SELECT    A.*,
          NVL("대출상환", 0) AS 대출상환,
          NVL("배당금현금", 0) AS 배당금현금,
          NVL("대출이자수령", 0) AS 대출이자수령
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('621010','621210')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "대출상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A3100' THEN A.STTL_AMT END) AS "배당금현금",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A7140' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "대출이자수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                     , CX_TP_BO_TRSC_TP B
           WHERE     FUND_CD IN ('621010','621210')
           AND       1=1
--                     AND A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           AND       A.TRSC_TP_CD IN ('A1110','A7140','A3100')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 대출상환 매도 
       , 0 매수
       , 대출이자수령 + 배당금현금 별도  
FROM FT
) SELECT STD_DT, '국내대체직접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_ai_j_soc = """
WITH FT AS (
SELECT    A.*,
          NVL("대출상환", 0) AS 대출상환,
          NVL("배당금현금", 0) AS 배당금현금,
          NVL("대출이자수령", 0) AS 대출이자수령
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('621010','621210')
                     AND CLAS_10_CD = 'AI110'
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "대출상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A3100' THEN A.STTL_AMT END) AS "배당금현금",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A7140' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "대출이자수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND FUND_CD IN ('621010','621210')
                     AND CLAS_10_CD = 'AI110'
--                     AND A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           AND       A.TRSC_TP_CD IN ('A1110','A7140','A3100')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 대출상환 매도 
       , 0 매수
       , 대출이자수령 + 배당금현금 별도  
FROM FT
) SELECT STD_DT, '국내대체직접_SOC' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## SOC
sql_dom_ai_j_realt = """
WITH FT AS (
SELECT    A.*,
          NVL("대출상환", 0) AS 대출상환,
          NVL("배당금현금", 0) AS 배당금현금,
          NVL("대출이자수령", 0) AS 대출이자수령
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1 
                     AND FUND_CD IN ('621010','621210')
                     AND CLAS_10_CD = 'AI210'
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "대출상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A3100' THEN A.STTL_AMT END) AS "배당금현금",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'A7140' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "대출이자수령"
           FROM      CX_TP_ACNT_BY_TRBD A
--                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND FUND_CD IN ('621010','621210')
                     AND CLAS_10_CD = 'AI210'
--                     AND A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           AND       A.TRSC_TP_CD IN ('A1110','A7140','A3100')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 대출상환 매도 
       , 0 매수
       , 대출이자수령 + 배당금현금 별도  
FROM FT
) SELECT STD_DT, '국내대체직접_부동산' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 부동산

## 국내대체간접

sql_dom_ai_g = """
WITH FT AS (
SELECT    A.*,
          NVL("간접상품상환",0) AS 간접상품상환,
          NVL("간접상품매수",0) AS 간접상품매수,          
          NVL("간접상품추가투자",0) AS 간접상품추가투자,
          NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정          
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_10_CD IN (SELECT PDT_MCTG_CD
                                        FROM CX_TP_PDT_MCTG_CD
                                        WHERE 1=1
                                              AND PDT_MCTG_CD LIKE '%AI%'
                                              AND PDT_MCTG_CD_NM LIKE '%국내%' 
                                              AND PDT_MCTG_CD_NM LIKE '%간접%')
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630' 
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"                                          
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_10_CD IN (SELECT PDT_MCTG_CD
                                        FROM CX_TP_PDT_MCTG_CD
                                        WHERE 1=1
                                              AND PDT_MCTG_CD LIKE '%AI%'
                                              AND PDT_MCTG_CD_NM LIKE '%국내%' 
                                              AND PDT_MCTG_CD_NM LIKE '%간접%')           
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내대체간접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_dom_ai_g_soc = """
WITH FT AS (
SELECT    A.*,
          NVL("간접상품상환",0) AS 간접상품상환,
          NVL("간접상품매수",0) AS 간접상품매수,          
          NVL("간접상품추가투자",0) AS 간접상품추가투자,
          NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정          
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_10_CD IN 'AI130'
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630' 
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"                                          
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_10_CD IN 'AI130'           
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내대체간접_SOC' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## SOC
sql_dom_ai_g_realt = """
WITH FT AS (
SELECT    A.*,
          NVL("간접상품상환",0) AS 간접상품상환,
          NVL("간접상품매수",0) AS 간접상품매수,          
          NVL("간접상품추가투자",0) AS 간접상품추가투자,
          NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정          
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_10_CD IN 'AI230'
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630' 
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"                                          
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_10_CD IN 'AI230'           
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내대체간접_부동산' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 부동산
sql_dom_ai_g_pe = """
WITH FT AS (
SELECT    A.*,
          NVL("간접상품상환",0) AS 간접상품상환,
          NVL("간접상품매수",0) AS 간접상품매수,          
          NVL("간접상품추가투자",0) AS 간접상품추가투자,
          NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정          
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_10_CD IN ('AI360', 'AI330')
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630' 
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"                                          
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_10_CD IN ('AI360', 'AI330')           
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내대체간접_PEF' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## PEF
sql_dom_ai_g_etc = """
WITH FT AS (
SELECT    A.*,
          NVL("간접상품상환",0) AS 간접상품상환,
          NVL("간접상품매수",0) AS 간접상품매수,          
          NVL("간접상품추가투자",0) AS 간접상품추가투자,
          NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정          
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_10_CD IN ('AI430')
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630' 
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"                                          
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_10_CD IN ('AI430')       
                     AND FUND_CD IN ('621110','631110')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
           AND       A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
           GROUP BY  A.STD_DT) B
WHERE 1=1
AND     A.STD_DT = B.STD_DT(+)
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '국내대체간접_기타' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 기타

## 해외대체간접

sql_ov_ai_g = """
WITH FT AS (
SELECT    A.STD_DT
          , NVL("평가금액",0) AS 평가금액
          , NVL("장부금액",0) AS 장부금액
          , NVL("간접상품상환",0) AS 간접상품상환
          , NVL("간접상품매수",0) AS 간접상품매수          
          , NVL("간접상품추가투자",0) AS 간접상품추가투자
          , NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI140','AI240','AI340','AI640', 'AI140','AI240','AI340','AI640')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD IN ('AI140','AI240','AI340','AI640', 'AI140','AI240','AI340','AI640') 
                     AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           GROUP BY  A.STD_DT) B
--           ,(SELECT DISTINCT STD_DT 
--            FROM CX_TP_ACNT_BY_PSBD) C
WHERE     A.STD_DT = B.STD_DT(+)
--          AND A.STD_DT(+)=C.STD_DT
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외대체간접' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
"""
sql_ov_ai_g_soc = """
WITH FT AS (
SELECT    A.STD_DT
          , NVL("평가금액",0) AS 평가금액
          , NVL("장부금액",0) AS 장부금액
          , NVL("간접상품상환",0) AS 간접상품상환
          , NVL("간접상품매수",0) AS 간접상품매수          
          , NVL("간접상품추가투자",0) AS 간접상품추가투자
          , NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI140')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI140') 
                     AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           GROUP BY  A.STD_DT) B
--           ,(SELECT DISTINCT STD_DT 
--            FROM CX_TP_ACNT_BY_PSBD) C
WHERE     A.STD_DT = B.STD_DT(+)
--          AND A.STD_DT(+)=C.STD_DT
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외대체간접_SOC' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## SOC
sql_ov_ai_g_realt = """
WITH FT AS (
SELECT    A.STD_DT
          , NVL("평가금액",0) AS 평가금액
          , NVL("장부금액",0) AS 장부금액
          , NVL("간접상품상환",0) AS 간접상품상환
          , NVL("간접상품매수",0) AS 간접상품매수          
          , NVL("간접상품추가투자",0) AS 간접상품추가투자
          , NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI240')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI240') 
                     AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           GROUP BY  A.STD_DT) B
--           ,(SELECT DISTINCT STD_DT 
--            FROM CX_TP_ACNT_BY_PSBD) C
WHERE     A.STD_DT = B.STD_DT(+)
--          AND A.STD_DT(+)=C.STD_DT
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외대체간접_부동산' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 부동산
sql_ov_ai_g_pe = """
WITH FT AS (
SELECT    A.STD_DT
          , NVL("평가금액",0) AS 평가금액
          , NVL("장부금액",0) AS 장부금액
          , NVL("간접상품상환",0) AS 간접상품상환
          , NVL("간접상품매수",0) AS 간접상품매수          
          , NVL("간접상품추가투자",0) AS 간접상품추가투자
          , NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI340')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI340') 
                     AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           GROUP BY  A.STD_DT) B
--           ,(SELECT DISTINCT STD_DT 
--            FROM CX_TP_ACNT_BY_PSBD) C
WHERE     A.STD_DT = B.STD_DT(+)
--          AND A.STD_DT(+)=C.STD_DT
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외대체간접_PE' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## PEF
sql_ov_ai_g_hedge = """
WITH FT AS (
SELECT    A.STD_DT
          , NVL("평가금액",0) AS 평가금액
          , NVL("장부금액",0) AS 장부금액
          , NVL("간접상품상환",0) AS 간접상품상환
          , NVL("간접상품매수",0) AS 간접상품매수          
          , NVL("간접상품추가투자",0) AS 간접상품추가투자
          , NVL("간접상품현금분배금수령",0) AS 간접상품현금분배금수령
--          , NVL("매매손익정정",0) AS 매매손익정정
FROM      ( --일별 평가금액
           SELECT    STD_DT,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
           FROM      CX_TP_ACNT_BY_PSBD
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI640')
                     AND STD_DT BETWEEN '20171231' AND '20180630'
           GROUP BY  STD_DT) A,
          ( --일별 거래금액
           SELECT    A.STD_DT,
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1110' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품상환",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O1100' THEN A.STTL_AMT END) AS "간접상품매수",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4400' THEN A.STTL_AMT END) AS "간접상품추가투자",
                     SUM(CASE WHEN A.TRSC_TP_CD = 'O4200' THEN A.STTL_AMT + A.ADPY_CRP_TAX END) AS "간접상품현금분배금수령"
--                     , SUM(CASE WHEN A.TRSC_TP_CD = 'X1500' THEN A.STTL_AMT  END) AS "매매손익정정"
           FROM      CX_TP_ACNT_BY_TRBD A
                     , CX_TP_BO_TRSC_TP B
           WHERE     1=1
                     AND CLAS_13_CD IN ('413010','621110','631110','611010')
                     AND CLAS_10_CD  IN ('AI640') 
                     AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                     AND A.TRSC_TP_CD IN ('O1110','O4400','O1100','O4200')
                     AND STD_DT BETWEEN '20171231' AND '20180630'                     
           GROUP BY  A.STD_DT) B
--           ,(SELECT DISTINCT STD_DT 
--            FROM CX_TP_ACNT_BY_PSBD) C
WHERE     A.STD_DT = B.STD_DT(+)
--          AND A.STD_DT(+)=C.STD_DT
ORDER BY  A.STD_DT
), SND AS (
-- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
       , 평가금액
       , 장부금액
       , 간접상품상환 매도 
       , 간접상품매수 + 간접상품추가투자 매수
       , 간접상품현금분배금수령 별도  
FROM FT
) SELECT STD_DT, '해외대체간접_헤지' 자산구분, 전일자평가금액, 평가금액
         , 장부금액, 매도, 매수, 별도
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                평가금액 + 매도 + 별도 
                ELSE 평가금액 + 별도 END 기말금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                전일자평가금액 + 매수 
                ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
                (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
                ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률                
FROM SND
WHERE STD_DT BETWEEN '20180101' AND '20180630'
""" ## 헤지펀드

# BM 및 무위험수익률 쿼리

sql_bm_all = """
SELECT A.STD_DT
       , 국내채권직접, 금융상품, 국내채권계 , 해외채권직접, 해외채권간접, 해외채권계 
       , A.국내채권계*B.BND_WGT + A.해외채권계*B.FRBN_WGT 채권계 
       , 국내주식직접, 국내주식간접, 국내주식계, 해외주식간접
       , 국내주식계*B.STK_WGT + A.해외주식간접*B.FRST_WGT 주식계
       , 현금성, 금융자산계
       , 국내대체직접, 국내대체간접, 국내대체, 해외대체간접, 대체전체, 기금전체       
FROM
(
    SELECT DT STD_DT         
           , MIN(CASE WHEN BM_CD = 'B0304' THEN RET + 1 END) 국내채권직접       
           , MIN(CASE WHEN BM_CD = 'B0304' THEN RET + 1 END) 금융상품       
           , MIN(CASE WHEN BM_CD = 'B0304' THEN RET + 1 END) 국내채권계
           , MIN(CASE WHEN BM_CD = 'B0271' THEN RET + 1 END) 해외채권직접       
           , MIN(CASE WHEN BM_CD = 'B0271' THEN RET + 1 END) 해외채권간접
           , MIN(CASE WHEN BM_CD = 'B0271' THEN RET + 1 END) 해외채권계              
           , MIN(CASE WHEN BM_CD = 'B0273' THEN RET + 1 END) 국내주식직접           
           , MIN(CASE WHEN BM_CD = 'B0272' THEN RET + 1 END) 국내주식간접
           , MIN(CASE WHEN BM_CD = 'B0272' THEN RET + 1 END) 국내주식계       
           , MIN(CASE WHEN BM_CD = 'B0302' THEN RET + 1 END) 해외주식간접  
           , MIN(CASE WHEN BM_CD = 'B0266' THEN RET + 1 END) 현금성       
           , MIN(CASE WHEN BM_CD = 'B0306' THEN RET + 1 END) 금융자산계             
           , MIN(CASE WHEN BM_CD = 'B0339' THEN RET + 1 END) 국내대체직접       
           , MIN(CASE WHEN BM_CD = 'B0340' THEN RET + 1 END) 국내대체간접       
           , MIN(CASE WHEN BM_CD = 'B0329' THEN RET + 1 END) 국내대체       
           , MIN(CASE WHEN BM_CD = 'B0342' THEN RET + 1 END) 해외대체간접
           , MIN(CASE WHEN BM_CD = 'B0331' THEN RET + 1 END) 대체전체
           , MIN(CASE WHEN BM_CD = 'B0305' THEN RET + 1 END) 기금전체   
    FROM CX_TP_MRET
    WHERE DT BETWEEN '20180101' AND '20180630'
    GROUP BY DT
) A, 
(
        SELECT A.STD_DT
               , NVL(LAST_VALUE(NULLIF(B.BND_WGT/(B.BND_WGT + B.FRBN_WGT), 0))
                    IGNORE NULLS OVER (ORDER BY A.STD_DT), 0) BND_WGT 
               , NVL(LAST_VALUE(NULLIF(B.FRBN_WGT/(B.BND_WGT + B.FRBN_WGT), 0))
                    IGNORE NULLS OVER (ORDER BY A.STD_DT), 0) FRBN_WGT     
               , NVL(LAST_VALUE(NULLIF(B.STK_WGT/(B.STK_WGT + B.FRST_WGT), 0))
                    IGNORE NULLS OVER (ORDER BY A.STD_DT), 0) STK_WGT 
               , NVL(LAST_VALUE(NULLIF(B.FRST_WGT/(B.STK_WGT + B.FRST_WGT), 0))
                    IGNORE NULLS OVER (ORDER BY A.STD_DT), 0) FRST_WGT                                    
        FROM
        (
            SELECT TRD_DT AS STD_DT
            FROM FNC_CALENDAR@D_FNDB2_UFNGDBA
            WHERE TRD_DT BETWEEN '20180101' AND '20180630'
        ) A, 
        (
            SELECT BIGN_DT, BND_WGT, FRBN_WGT, STK_WGT, FRST_WGT
            FROM CX_TP_BMWT
            WHERE 1=1 
                  AND BM_CD = 'B0305'
                  AND BIGN_DT BETWEEN '20180101' AND '20180630'
        ) B
        WHERE A.STD_DT = B.BIGN_DT(+)
) B
WHERE A.STD_DT = B.STD_DT
ORDER BY A.STD_DT
"""
sql_rf = """
SELECT A.TRD_DT AS STD_DT
        , nvl(last_value(nullif(B.AMOUNT, 0)) 
              IGNORE NULLS OVER (ORDER BY A.TRD_DT), 0) RF
FROM
(
    SELECT TRD_DT
    FROM FNC_CALENDAR@D_FNDB2_UFNGDBA
    WHERE TRD_DT BETWEEN '20171228' AND '20180630'
) A, 
(
    SELECT TRD_DT, AMOUNT
    FROM FNE_ECO_DATA@D_FNDB2_UFNGDBA
    WHERE 1=1
          AND ECO_CD = '11.02.003.009'
          AND TERM = 'D'
          AND TRD_DT BETWEEN '20161228' AND '20180630'  
) B
WHERE 1=1 
      AND A.TRD_DT = B.TRD_DT(+)
      """

## BM 비중

sql_bm_wgt = """

"""

## Start

qry = query()

## 자산군 기초데이터

dm_bd_j = qry.read_sql(sql_dom_bd_j)

dm_bd_j_sxm = qry.read_sql(sql_dom_bd_sxm)
dm_bd_j_sxmoy = qry.read_sql(sql_dom_bd_sxmoy)
dm_bd_j_oytw = qry.read_sql(sql_dom_bd_oytw)
dm_bd_j_twth = qry.read_sql(sql_dom_bd_twth)
dm_bd_j_thfv = qry.read_sql(sql_dom_bd_thfv)
dm_bd_j_fvty = qry.read_sql(sql_dom_bd_fvty)
dm_bd_j_tytw = qry.read_sql(sql_dom_bd_tytwy)
dm_bd_j_twy = qry.read_sql(sql_dom_bd_twy)

## 국고채

dm_bd_j_tr = qry.read_sql(sql_dom_bd_tr)

dm_bd_j_tr_sxm = qry.read_sql(sql_dom_bd_tr_sxm)
dm_bd_j_tr_sxmoy = qry.read_sql(sql_dom_bd_tr_sxmoy)
dm_bd_j_tr_oytw = qry.read_sql(sql_dom_bd_tr_oytw)
dm_bd_j_tr_twth = qry.read_sql(sql_dom_bd_tr_twth)
dm_bd_j_tr_thfv = qry.read_sql(sql_dom_bd_tr_thfv)
dm_bd_j_tr_fvty = qry.read_sql(sql_dom_bd_tr_fvty)
dm_bd_j_tr_tytw = qry.read_sql(sql_dom_bd_tr_tytwy)
dm_bd_j_tr_twy = qry.read_sql(sql_dom_bd_tr_twy)

## 특수채

dm_bd_j_gse = qry.read_sql(sql_dom_bd_gse)

dm_bd_j_gse_sxm = qry.read_sql(sql_dom_bd_gse_sxm)
dm_bd_j_gse_sxmoy = qry.read_sql(sql_dom_bd_gse_sxmoy)
dm_bd_j_gse_oytw = qry.read_sql(sql_dom_bd_gse_oytw)
dm_bd_j_gse_twth = qry.read_sql(sql_dom_bd_gse_twth)
dm_bd_j_gse_thfv = qry.read_sql(sql_dom_bd_gse_thfv)
dm_bd_j_gse_fvty = qry.read_sql(sql_dom_bd_gse_fvty)
dm_bd_j_gse_tytw = qry.read_sql(sql_dom_bd_gse_tytwy)
dm_bd_j_gse_twy = qry.read_sql(sql_dom_bd_gse_twy)

## 금융채

dm_bd_j_fnb = qry.read_sql(sql_dom_bd_fnb)

dm_bd_j_fnb_sxm = qry.read_sql(sql_dom_bd_fnb_sxm)
dm_bd_j_fnb_sxmoy = qry.read_sql(sql_dom_bd_fnb_sxmoy)
dm_bd_j_fnb_oytw = qry.read_sql(sql_dom_bd_fnb_oytw)
dm_bd_j_fnb_twth = qry.read_sql(sql_dom_bd_fnb_twth)
dm_bd_j_fnb_thfv = qry.read_sql(sql_dom_bd_fnb_thfv)
dm_bd_j_fnb_fvty = qry.read_sql(sql_dom_bd_fnb_fvty)
dm_bd_j_fnb_tytw = qry.read_sql(sql_dom_bd_fnb_tytwy)
dm_bd_j_fnb_twy = qry.read_sql(sql_dom_bd_fnb_twy)

## 회사채

dm_bd_j_corp = qry.read_sql(sql_dom_bd_corp)

dm_bd_j_corp_sxm = qry.read_sql(sql_dom_bd_corp_sxm)
dm_bd_j_corp_sxmoy = qry.read_sql(sql_dom_bd_corp_sxmoy)
dm_bd_j_corp_oytw = qry.read_sql(sql_dom_bd_corp_oytw)
dm_bd_j_corp_twth = qry.read_sql(sql_dom_bd_corp_twth)
dm_bd_j_corp_thfv = qry.read_sql(sql_dom_bd_corp_thfv)
dm_bd_j_corp_fvty = qry.read_sql(sql_dom_bd_corp_fvty)
dm_bd_j_corp_tytw = qry.read_sql(sql_dom_bd_corp_tytwy)
dm_bd_j_corp_twy = qry.read_sql(sql_dom_bd_corp_twy)

"""
_j: 직접
_g: 간접
_all: 전체
"""

## 금융상품
dm_bd_fn = qry.read_sql(sql_dom_fn)
## 국내채권전체
dm_bd_all = data_aggr([dm_bd_j, dm_bd_fn]).aggr_dt('국내채권계')

"""
국내채권간접은 2015년 기준으로 모두 상환되었으므로 직접과 금융상품 밖에 없음 
"""

## 해외채권 직접(_j) / 간접(_g) / 전체(_all)
ov_bd_j = qry.read_sql(sql_ov_bd_j)
ov_bd_g = qry.read_sql(sql_ov_bd_g)
ov_bd_all = data_aggr([ov_bd_j, ov_bd_g]).aggr_dt('해외채권계')
## 채권전체
bd_all = data_aggr([dm_bd_all, ov_bd_all]).aggr_dt('채권계')
## 국내주식 직접 / 간접(위탁) / 간접 유형 / 전체
dm_stk_j = qry.read_sql(sql_dom_stk_j)
dm_stk_g = qry.read_sql(sql_dom_stk_g)
dm_stk_g_grw = qry.read_sql(sql_dom_stk_g_grw)
dm_stk_g_idx = qry.read_sql(sql_dom_stk_g_idx)
dm_stk_g_smb = qry.read_sql(sql_dom_stk_g_smb)
dm_stk_g_div = qry.read_sql(sql_dom_stk_g_div)
dm_stk_g_esg = qry.read_sql(sql_dom_stk_g_esg)
dm_stk_g_acq = qry.read_sql(sql_dom_stk_g_acq)
dm_stk_g_val = qry.read_sql(sql_dom_stk_g_val)
dm_stk_all = data_aggr([dm_stk_j, dm_stk_g]).aggr_dt('국내주식계')
## 해외주식간접
ov_stk_g = qry.read_sql(sql_ov_stk_g)
ov_stk_g_act = qry.read_sql(sql_ov_stk_g_act)
ov_stk_g_psv = qry.read_sql(sql_ov_stk_g_psv)
## 주식 전체
stk_all = data_aggr([dm_stk_all, ov_stk_g]).aggr_dt('주식계')
## 현금성자산(채권 금융상품과 다르므로 유의할 것)
csh_j = qry.read_sql(sql_csh)
## 대체자산을 제외한 전체
fn_ast = data_aggr([dm_stk_all, ov_stk_g, bd_all, csh_j]).aggr_dt('금융자산계')
## 국내 대체투자 직접
dom_ai_j = qry.read_sql(sql_dom_ai_j)
dom_ai_j_soc = qry.read_sql(sql_dom_ai_j_soc)
dom_ai_j_realt = qry.read_sql(sql_dom_ai_j_realt)
## 국내 대체투자 간접
dom_ai_g = qry.read_sql(sql_dom_ai_g)
dom_ai_g_soc = qry.read_sql(sql_dom_ai_g_soc)
dom_ai_g_realt = qry.read_sql(sql_dom_ai_g_realt)
dom_ai_g_pe = qry.read_sql(sql_dom_ai_g_pe)
dom_ai_g_etc = qry.read_sql(sql_dom_ai_g_etc)
## 국내 대체
dom_ai_all = data_aggr([dom_ai_j, dom_ai_g]).aggr_dt('국내대체')
dom_ai_all_soc = data_aggr([dom_ai_j_soc, dom_ai_g_soc]).aggr_dt('국내대체_SOC')
dom_ai_all_realt = data_aggr([dom_ai_j_realt, dom_ai_g_realt]).aggr_dt('국내대체_부동산')
## 해외대체 간접(직접은 없으므로 간접 전체는 해외대체 전체)
ov_ai_g = qry.read_sql(sql_ov_ai_g)
ov_ai_g_soc = qry.read_sql(sql_ov_ai_g_soc)
ov_ai_g_realt = qry.read_sql(sql_ov_ai_g_realt)
ov_ai_g_pe = qry.read_sql(sql_ov_ai_g_pe)
ov_ai_g_hedge = qry.read_sql(sql_ov_ai_g_hedge)
## 대체자산 전체
ai_all = data_aggr([dom_ai_all, ov_ai_g]).aggr_dt('대체전체')
all_ast = data_aggr([fn_ast, ai_all]).aggr_dt('기금전체')

## 무위험수익률 / BM

bm_rt = qry.read_sql(sql_bm_all)
rf_rt = qry.rf_read_sql(sql_rf)

rf_rt.to_csv('New_Data/rf_rt.csv', encoding='cp949')

## 기초데이터 정리
"""
'전체 일별'
.rt: 수익률
.book_val: 장부금액
.mkt_val: 시가금액
.net_buy: 순매수
.bf_ast_rto: 일별 기초자산 비중(성과요인분해시 사용) 
.book_rto: 장부자산비중(말잔 비중 구할 때 사용)
"""


## 국내채권직접 - 섹터별

feat_dm_j_bd = feat_extraction([dm_bd_j, dm_bd_j_tr, dm_bd_j_gse, dm_bd_j_fnb, dm_bd_j_corp])

tp_dm_bd_j_rt = feat_dm_j_bd.rt()
tp_dm_bd_j_book_val = feat_dm_j_bd.book_val()
tp_dm_bd_j_mkt_val = feat_dm_j_bd.mkt_val()
tp_dm_bd_j_net_buy = feat_dm_j_bd.net_buy()
tp_dm_bd_j_rto = feat_dm_j_bd.bf_ast_rto(st_cols = '국내채권직접')
tp_dm_bd_j_book_rto = feat_dm_j_bd.book_val_rto(st_cols = '국내채권직접')

tp_dm_bd_j_rt.to_csv('New_Data/tp_dm_bd_j_rt.csv', encoding='cp949')
tp_dm_bd_j_rto.to_csv('New_Data/tp_dm_bd_j_rto.csv', encoding='cp949')

## 국내채권직접 - 만기별

feat_dm_j_bd_mat = feat_extraction([dm_bd_j, dm_bd_j_sxm, dm_bd_j_sxmoy, dm_bd_j_oytw
                                    , dm_bd_j_twth, dm_bd_j_thfv, dm_bd_j_fvty
                                    , dm_bd_j_tytw, dm_bd_j_twy])

tp_dm_j_bd_mat_rt = feat_dm_j_bd_mat.rt()
tp_dm_j_bd_mat_book_val = feat_dm_j_bd_mat.book_val()
tp_dm_j_bd_mat_mkt_val = feat_dm_j_bd_mat.mkt_val()
tp_dm_j_bd_mat_net_buy = feat_dm_j_bd_mat.net_buy()
tp_dm_j_bd_mat_rto = feat_dm_j_bd_mat.bf_ast_rto(st_cols = '국내채권직접')
tp_dm_j_bd_mat_book_rto = feat_dm_j_bd_mat.book_val_rto(st_cols = '국내채권직접')


tp_dm_j_bd_mat_book_rto.to_csv('New_Data/tp_dm_j_bd_mat_book_rto.csv', encoding='cp949')
tp_dm_j_bd_mat_rto.to_csv('New_Data/tp_dm_j_bd_mat_rto.csv', encoding='cp949')
tp_dm_j_bd_mat_rt.to_csv('New_Data/tp_dm_j_bd_mat_rt.csv', encoding='cp949')

## 국내채권직접_국고채 - 만기별

feat_dm_j_bd_tr = feat_extraction([dm_bd_j, dm_bd_j_tr_sxm, dm_bd_j_tr_sxmoy
                                    , dm_bd_j_tr_oytw, dm_bd_j_tr_twth, dm_bd_j_tr_thfv
                                    , dm_bd_j_tr_fvty, dm_bd_j_tr_tytw, dm_bd_j_tr_twy])

tp_dm_bd_j_tr_rt = feat_dm_j_bd_tr.rt()
tp_dm_bd_j_tr_book_val = feat_dm_j_bd_tr.book_val()
tp_dm_bd_j_tr_mkt_val = feat_dm_j_bd_tr.mkt_val()
tp_dm_bd_j_tr_net_buy = feat_dm_j_bd_tr.net_buy()
tp_dm_bd_j_tr_rto = feat_dm_j_bd_tr.bf_ast_rto(st_cols = '국내채권직접')
tp_dm_bd_j_tr_book_rto = feat_dm_j_bd_tr.book_val_rto(st_cols = '국내채권직접')

tp_dm_bd_j_tr_rto.to_csv('New_Data/tp_dm_bd_j_tr_rto.csv', encoding='cp949')
tp_dm_bd_j_tr_rt.to_csv('New_Data/tp_dm_bd_j_tr_rt.csv', encoding='cp949')

## 국내채권직접_특수채 - 만기별

feat_dm_j_bd_gse = feat_extraction([dm_bd_j, dm_bd_j_gse_sxm, dm_bd_j_gse_sxmoy
                                    , dm_bd_j_gse_oytw, dm_bd_j_gse_twth, dm_bd_j_gse_thfv
                                    , dm_bd_j_gse_fvty, dm_bd_j_gse_tytw, dm_bd_j_gse_twy])

tp_dm_bd_j_gse_rt = feat_dm_j_bd_gse.rt()
tp_dm_bd_j_gse_book_val = feat_dm_j_bd_gse.book_val()
tp_dm_bd_j_gse_mkt_val = feat_dm_j_bd_gse.mkt_val()
tp_dm_bd_j_gse_net_buy = feat_dm_j_bd_gse.net_buy()
tp_dm_bd_j_gse_rto = feat_dm_j_bd_gse.bf_ast_rto(st_cols = '국내채권직접')
tp_dm_bd_j_gse_book_rto = feat_dm_j_bd_gse.book_val_rto(st_cols = '국내채권직접')

tp_dm_bd_j_gse_rto.to_csv('New_Data/tp_dm_bd_j_gse_rto.csv', encoding='cp949')
tp_dm_bd_j_gse_rt.to_csv('New_Data/tp_dm_bd_j_gse_rt.csv', encoding='cp949')

## 국내채권직접_금융채 - 만기별

feat_dm_j_bd_fnb = feat_extraction([dm_bd_j, dm_bd_j_fnb_sxm, dm_bd_j_fnb_sxmoy
                                    , dm_bd_j_fnb_oytw, dm_bd_j_fnb_twth, dm_bd_j_fnb_thfv
                                    , dm_bd_j_fnb_fvty, dm_bd_j_fnb_tytw, dm_bd_j_fnb_twy])

tp_dm_bd_j_fnb_rt = feat_dm_j_bd_fnb.rt()
tp_dm_bd_j_fnb_book_val = feat_dm_j_bd_fnb.book_val()
tp_dm_bd_j_fnb_mkt_val = feat_dm_j_bd_fnb.mkt_val()
tp_dm_bd_j_fnb_net_buy = feat_dm_j_bd_fnb.net_buy()
tp_dm_bd_j_fnb_rto = feat_dm_j_bd_fnb.bf_ast_rto(st_cols = '국내채권직접')
tp_dm_bd_j_fnb_book_rto = feat_dm_j_bd_fnb.book_val_rto(st_cols = '국내채권직접')

tp_dm_bd_j_fnb_rto.to_csv('New_Data/tp_dm_bd_j_fnb_rto.csv', encoding='cp949')
tp_dm_bd_j_fnb_rt.to_csv('New_Data/tp_dm_bd_j_fnb_rt.csv', encoding='cp949')

## 국내채권직접_회사채 - 만기별

feat_dm_j_bd_corp = feat_extraction([dm_bd_j, dm_bd_j_corp_sxm, dm_bd_j_corp_sxmoy
                                    , dm_bd_j_corp_oytw, dm_bd_j_corp_twth, dm_bd_j_corp_thfv
                                    , dm_bd_j_corp_fvty, dm_bd_j_corp_tytw, dm_bd_j_corp_twy])

tp_dm_bd_j_corp_rt = feat_dm_j_bd_corp.rt()
tp_dm_bd_j_corp_book_val = feat_dm_j_bd_corp.book_val()
tp_dm_bd_j_corp_mkt_val = feat_dm_j_bd_corp.mkt_val()
tp_dm_bd_j_corp_net_buy = feat_dm_j_bd_corp.net_buy()
tp_dm_bd_j_corp_rto = feat_dm_j_bd_corp.bf_ast_rto(st_cols = '국내채권직접')
tp_dm_bd_j_corp_book_rto = feat_dm_j_bd_corp.book_val_rto(st_cols = '국내채권직접')

tp_dm_bd_j_corp_rto.to_csv('New_Data/tp_dm_bd_j_corp_rto.csv', encoding='cp949')
tp_dm_bd_j_corp_rt.to_csv('New_Data/tp_dm_bd_j_corp_rt.csv', encoding='cp949')

## 국내주식간접

feat_dm_all_stk = feat_extraction([dm_stk_all, dm_stk_g, dm_stk_j, dm_stk_g_grw, dm_stk_g_idx,
                                 dm_stk_g_smb, dm_stk_g_esg, dm_stk_g_div, dm_stk_g_val, dm_stk_g_acq])

tp_dm_all_stk_rt = feat_dm_all_stk.rt()
tp_dm_all_stk_book_val = feat_dm_all_stk.book_val()
tp_dm_all_stk_mkt_val = feat_dm_all_stk.mkt_val()
tp_dm_all_stk_b_mkt_val = feat_dm_all_stk.b_mkt_val()

tp_dm_all_stk_bf_ast = feat_dm_all_stk.bf_ast_val()
tp_dm_all_stk_eval = feat_dm_all_stk.af_ast_val()
tp_dm_all_stk_buy = feat_dm_all_stk.buy()
tp_dm_all_stk_sell = feat_dm_all_stk.sell()
tp_dm_all_stk_other = feat_dm_all_stk.other()

tp_dm_all_stk_net_buy = feat_dm_all_stk.net_buy()
tp_dm_all_stk_rto = feat_dm_all_stk.bf_ast_rto(st_cols = '국내주식간접')
tp_dm_all_stk_book_rto = feat_dm_all_stk.book_val_rto(st_cols = '국내주식간접')

bm_dm_stk_all = pd.concat([bm_rt['국내주식간접'].rename('국내주식간접')
                           , bm_rt['국내주식간접'].rename('국내주식간접_가치')
                           , bm_rt['국내주식간접'].rename('국내주식간접_배당')
                           , bm_rt['국내주식간접'].rename('국내주식간접_사회책임')
                           , bm_rt['국내주식간접'].rename('국내주식간접_성장')
                           , bm_rt['국내주식간접'].rename('국내주식간접_인덱스')
                           , bm_rt['국내주식간접'].rename('국내주식간접_중소형')
                           , bm_rt['국내주식간접'].rename('국내주식간접_액티브퀀트')
                           , bm_rt['국내주식간접'].rename('국내주식계')
                           , bm_rt['국내주식직접'].rename('국내주식직접')], axis = 1)

tp_dm_all_stk_rt.to_csv('New_Data/tp_dm_all_stk_rt.csv', encoding='cp949')
tp_dm_all_stk_book_val.to_csv('New_Data/tp_dm_all_stk_book_val.csv', encoding='cp949')
tp_dm_all_stk_book_rto.to_csv('New_Data/tp_dm_all_stk_book_rto.csv', encoding='cp949')
tp_dm_all_stk_buy.to_csv('New_Data/tp_dm_all_stk_buy.csv', encoding='cp949')
tp_dm_all_stk_sell.to_csv('New_Data/tp_dm_all_stk_sell.csv', encoding='cp949')
tp_dm_all_stk_other.to_csv('New_Data/tp_dm_all_stk_other.csv', encoding='cp949')
tp_dm_all_stk_bf_ast.to_csv('New_Data/tp_dm_all_stk_bf_ast.csv', encoding='cp949')
tp_dm_all_stk_mkt_val.to_csv('New_Data/tp_dm_all_stk_mkt_val.csv', encoding='cp949')
tp_dm_all_stk_b_mkt_val.to_csv('New_Data/tp_dm_all_stk_b_mkt_val.csv', encoding='cp949')
bm_dm_stk_all.to_csv('New_Data/bm_dm_stk_all.csv', encoding='cp949')

## 해외주식간접

feat_ov_g_stk = feat_extraction([ov_stk_g, ov_stk_g_act, ov_stk_g_psv])
tp_ov_stk_g_rt = feat_ov_g_stk.rt()
tp_ov_stk_g_book_val = feat_ov_g_stk.book_val()
tp_ov_stk_g_mkt_val = feat_ov_g_stk.mkt_val()
tp_ov_stk_g_net_buy = feat_ov_g_stk.net_buy()
tp_ov_stk_g_rto = feat_ov_g_stk.bf_ast_rto(st_cols = '해외주식간접')
tp_ov_stk_g_book_rto = feat_ov_g_stk.book_val_rto(st_cols = '해외주식간접')

bm_ov_stk_all = pd.concat([bm_rt['해외주식간접'].rename('해외주식간접')
                           , bm_rt['해외주식간접'].rename('해외주식간접_액티브')
                           , bm_rt['해외주식간접'].rename('해외주식간접_패시브')], axis = 1)

tp_ov_stk_g_rt.to_csv('New_Data/tp_ov_stk_g_rt.csv', encoding='cp949')
tp_ov_stk_g_book_val.to_csv('New_Data/tp_ov_stk_g_book_val.csv', encoding='cp949')
tp_ov_stk_g_book_rto.to_csv('New_Data/tp_ov_stk_g_book_rto.csv', encoding='cp949')
bm_ov_stk_all.to_csv('New_Data/bm_ov_stk_all.csv', encoding='cp949')


feat_dm_all_ai = feat_extraction([dom_ai_all, dom_ai_all_soc, dom_ai_all_realt, dom_ai_g_pe, dom_ai_g_etc])
tp_dm_ai_all_rt = feat_dm_all_ai.rt()
tp_dm_ai_all_book_val = feat_dm_all_ai.book_val()
tp_dm_ai_all_mkt_val = feat_dm_all_ai.mkt_val()
tp_dm_ai_all_net_buy = feat_dm_all_ai.net_buy()
tp_dm_ai_all_rto = feat_dm_all_ai.bf_ast_rto(st_cols = '국내대체')
tp_dm_ai_all_book_rto = feat_dm_all_ai.book_val_rto(st_cols = '국내대체')

bm_dm_ai_all = pd.concat([bm_rt['국내대체'].rename('국내대체')
                           , bm_rt['국내대체'].rename('국내대체_SOC')
                           , bm_rt['국내대체'].rename('국내대체_부동산')
                           , bm_rt['국내대체'].rename('국내대체간접_PEF')
                           , bm_rt['국내대체'].rename('국내대체간접_기타')], axis = 1)


tp_dm_ai_all_book_val.to_csv('New_Data/tp_dm_ai_all_book_val.csv', encoding='cp949')
tp_dm_ai_all_book_rto.to_csv('New_Data/tp_dm_ai_all_book_rto.csv', encoding='cp949')
tp_dm_ai_all_rt.to_csv('New_Data/tp_dm_ai_all_rt.csv', encoding='cp949')
bm_dm_ai_all.to_csv('New_Data/bm_dm_ai_all.csv', encoding='cp949')

feat_ov_all_ai = feat_extraction([ov_ai_g, ov_ai_g_soc, ov_ai_g_realt, ov_ai_g_pe, ov_ai_g_hedge])

tp_ov_ai_all_rt = feat_ov_all_ai.rt()
tp_ov_ai_all_book_val = feat_ov_all_ai.book_val()
tp_ov_ai_all_mkt_val = feat_ov_all_ai.mkt_val()
tp_ov_ai_all_net_buy = feat_ov_all_ai.net_buy()
tp_ov_ai_all_rto = feat_ov_all_ai.bf_ast_rto(st_cols = '해외대체간접')
tp_ov_ai_all_book_rto = feat_ov_all_ai.book_val_rto(st_cols = '해외대체간접')

bm_ov_ai_all = pd.concat([bm_rt['해외대체간접'].rename('해외대체간접')
                           , bm_rt['해외대체간접'].rename('해외대체간접_PE')
                           , bm_rt['해외대체간접'].rename('해외대체간접_SOC')
                           , bm_rt['해외대체간접'].rename('해외대체간접_부동산')
                           , bm_rt['해외대체간접'].rename('해외대체간접_헤지')], axis = 1)

tp_ov_ai_all_book_val.to_csv('New_Data/tp_ov_ai_all_book_val.csv', encoding='cp949')
tp_ov_ai_all_book_rto.to_csv('New_Data/tp_ov_ai_all_book_rto.csv', encoding='cp949')
tp_ov_ai_all_rt.to_csv('New_Data/tp_ov_ai_all_rt.csv', encoding='cp949')
bm_ov_ai_all.to_csv('New_Data/bm_ov_ai_all.csv', encoding='cp949')



feat = feat_extraction([dm_bd_j, dm_bd_fn, dm_bd_all,
                 ov_bd_j, ov_bd_g, ov_bd_all,
                 bd_all, dm_stk_j, dm_stk_g,
                 dm_stk_all, ov_stk_g, stk_all,
                 csh_j, fn_ast, dom_ai_j, dom_ai_g,
                 dom_ai_all, ov_ai_g, ai_all, all_ast])

tp_all_rt = feat.rt()
tp_all_book = feat.book_val()
tp_all_mkt_eval = feat.mkt_val()
tp_all_buy = feat.buy()
tp_all_sell = feat.sell()
tp_all_net_buy = feat.net_buy()
tp_all_bf_ast_val = feat.bf_ast_val()
tp_all_af_ast_val = feat.af_ast_val()
tp_all_bf_ast_rto = feat.bf_ast_rto()
tp_all_book_rto = feat.book_val_rto()

tp_all_rt.to_csv('New_Data/tp_all_rt.csv', encoding='cp949')
tp_all_book.to_csv('New_Data/tp_all_book.csv', encoding='cp949')
tp_all_mkt_eval.to_csv('New_Data/tp_all_mkt.csv', encoding='cp949')
tp_all_buy.to_csv('New_Data/tp_all_buy.csv', encoding='cp949')
tp_all_sell.to_csv('New_Data/tp_all_sell.csv', encoding='cp949')
tp_all_net_buy.to_csv('New_Data/tp_all_net_buy.csv', encoding='cp949')
tp_all_bf_ast_val.to_csv('New_Data/bf_ast_val.csv', encoding='cp949')
tp_all_af_ast_val.to_csv('New_Data/tp_all_af_ast_val.csv', encoding='cp949')
tp_all_bf_ast_rto.to_csv('New_Data/tp_all_bf_ast_rto.csv', encoding='cp949')
tp_all_book_rto.to_csv('New_Data/tp_all_book_rto.csv', encoding='cp949')


bm_rt.to_csv('New_Data/bm_rt.csv', encoding='cp949')
## 수익률

"""
기간(반기 / 연간) 수익률 / 위험성과지표
_prod_rt: 누적수익률
_ret_vol: 변동성 / 표준편차
_trek_error: TE
_sharp: 샤프비율
_ir: 정보비율
_beta:앞파/베타 전부 포함 
_tryno: 트레이너 지수
"""


## 해외대체
Rt_Stat_ov_ai = rt_stat(ret=tp_ov_ai_all_rt, rf = rf_rt, bm = bm_ov_ai_all)

Rt_ov_ai_prod_rt = Rt_Stat_ov_ai.prod_ret() ## 누적수익률
Rt_ov_ai_ret_vol = Rt_Stat_ov_ai.ret_vol() ## 변동성
Rt_ov_ai_trek_error = Rt_Stat_ov_ai.trek_error() ## TE
Rt_ov_ai_ir = Rt_Stat_ov_ai.inf_rto() ## TE
Rt_ov_ai_shape = Rt_Stat_ov_ai.sharpe_rto() ## 샤프
Rt_ov_ai_beta = Rt_Stat_ov_ai.alp_beta() ## 알파 / 베타
Rt_ov_ai_tryno = Rt_Stat_ov_ai.tryn_rto() ## 트레이너


Rt_ov_ai_des_Step = pd.concat([Rt_ov_ai_prod_rt['rt'].T.squeeze().rename('운용수익률'), Rt_ov_ai_prod_rt['bm'].T.squeeze().rename('BM수익률')
                   , Rt_ov_ai_prod_rt['rt'].sub(Rt_ov_ai_prod_rt['bm']).T.squeeze().rename('초과수익률')
                   , Rt_ov_ai_ret_vol['rt'].T.squeeze().rename('운용변동성'), Rt_ov_ai_ret_vol['bm'].T.squeeze().rename('BM변동성')
                   , Rt_ov_ai_trek_error.T.squeeze().rename('TE'), Rt_ov_ai_shape['rt_shp'].T.squeeze().rename('운용샤프')
                   , Rt_ov_ai_shape['bm_shp'].T.squeeze().rename('BM샤프'), Rt_ov_ai_ir.T.squeeze().rename('정보비율')
                   , Rt_ov_ai_beta['alpha'].T.squeeze().rename('알파'), Rt_ov_ai_beta['beta'].T.squeeze().rename('베타')
                   , Rt_ov_ai_tryno.T.squeeze().rename('트레이너')], axis = 1)

Rt_ov_ai_des_Step.to_csv('New_Data/Rt_ov_ai_des_Step.csv', encoding='cp949')

## 국내대체
Rt_Stat_dm_ai = rt_stat(ret=tp_dm_ai_all_rt, rf = rf_rt, bm = bm_dm_ai_all)

Rt_dm_ai_prod_rt = Rt_Stat_dm_ai.prod_ret() ## 누적수익률
Rt_dm_ai_ret_vol = Rt_Stat_dm_ai.ret_vol() ## 변동성
Rt_dm_ai_trek_error = Rt_Stat_dm_ai.trek_error() ## TE
Rt_dm_ai_ir = Rt_Stat_dm_ai.inf_rto() ## TE
Rt_dm_ai_shape = Rt_Stat_dm_ai.sharpe_rto() ## 샤프
Rt_dm_ai_beta = Rt_Stat_dm_ai.alp_beta() ## 알파 / 베타
Rt_dm_ai_tryno = Rt_Stat_dm_ai.tryn_rto() ## 트레이너


Rt_dm_ai_des_Step = pd.concat([Rt_dm_ai_prod_rt['rt'].T.squeeze().rename('운용수익률'), Rt_dm_ai_prod_rt['bm'].T.squeeze().rename('BM수익률')
                   , Rt_dm_ai_prod_rt['rt'].sub(Rt_dm_ai_prod_rt['bm']).T.squeeze().rename('초과수익률')
                   , Rt_dm_ai_ret_vol['rt'].T.squeeze().rename('운용변동성'), Rt_dm_ai_ret_vol['bm'].T.squeeze().rename('BM변동성')
                   , Rt_dm_ai_trek_error.T.squeeze().rename('TE'), Rt_dm_ai_shape['rt_shp'].T.squeeze().rename('운용샤프')
                   , Rt_dm_ai_shape['bm_shp'].T.squeeze().rename('BM샤프'), Rt_dm_ai_ir.T.squeeze().rename('정보비율')
                   , Rt_dm_ai_beta['alpha'].T.squeeze().rename('알파'), Rt_dm_ai_beta['beta'].T.squeeze().rename('베타')
                   , Rt_dm_ai_tryno.T.squeeze().rename('트레이너')], axis = 1)

Rt_dm_ai_des_Step.to_csv('New_Data/Rt_dm_ai_des_Step.csv', encoding='cp949')


## 해외주식
Rt_Stat_ov_stk = rt_stat(ret=tp_ov_stk_g_rt, rf = rf_rt, bm = bm_ov_stk_all)

Rt_ov_stk_prod_rt = Rt_Stat_ov_stk.prod_ret() ## 누적수익률
Rt_ov_stk_ret_vol = Rt_Stat_ov_stk.ret_vol() ## 변동성
Rt_ov_stk_trek_error = Rt_Stat_ov_stk.trek_error() ## TE
Rt_ov_stk_ir = Rt_Stat_ov_stk.inf_rto() ## TE
Rt_ov_stk_shape = Rt_Stat_ov_stk.sharpe_rto() ## 샤프
Rt_ov_stk_beta = Rt_Stat_ov_stk.alp_beta() ## 알파 / 베타
Rt_ov_stk_tryno = Rt_Stat_ov_stk.tryn_rto() ## 트레이너


Rt_ov_stk_des_Step = pd.concat([Rt_ov_stk_prod_rt['rt'].T.squeeze().rename('운용수익률'), Rt_ov_stk_prod_rt['bm'].T.squeeze().rename('BM수익률')
                   , Rt_ov_stk_prod_rt['rt'].sub(Rt_ov_stk_prod_rt['bm']).T.squeeze().rename('초과수익률')
                   , Rt_ov_stk_ret_vol['rt'].T.squeeze().rename('운용변동성'), Rt_ov_stk_ret_vol['bm'].T.squeeze().rename('BM변동성')
                   , Rt_ov_stk_trek_error.T.squeeze().rename('TE'), Rt_ov_stk_shape['rt_shp'].T.squeeze().rename('운용샤프')
                   , Rt_ov_stk_shape['bm_shp'].T.squeeze().rename('BM샤프'), Rt_ov_stk_ir.T.squeeze().rename('정보비율')
                   , Rt_ov_stk_beta['alpha'].T.squeeze().rename('알파'), Rt_ov_stk_beta['beta'].T.squeeze().rename('베타')
                   , Rt_ov_stk_tryno.T.squeeze().rename('트레이너')], axis = 1)

Rt_ov_stk_des_Step.to_csv('New_Data/Rt_ov_stk_des_Step.csv', encoding='cp949')



## 국내주식
Rt_Stat_dm_stk = rt_stat(ret=tp_dm_all_stk_rt, rf = rf_rt, bm = bm_dm_stk_all)

Rt_dm_stk_prod_rt = Rt_Stat_dm_stk.prod_ret() ## 누적수익률
Rt_dm_stk_ret_vol = Rt_Stat_dm_stk.ret_vol() ## 변동성
Rt_dm_stk_trek_error = Rt_Stat_dm_stk.trek_error() ## TE
Rt_dm_stk_ir = Rt_Stat_dm_stk.inf_rto() ## TE
Rt_dm_stk_shape = Rt_Stat_dm_stk.sharpe_rto() ## 샤프
Rt_dm_stk_beta = Rt_Stat_dm_stk.alp_beta() ## 알파 / 베타
Rt_dm_stk_tryno = Rt_Stat_dm_stk.tryn_rto() ## 트레이너


Rt_dm_stk_des_Step = pd.concat([Rt_dm_stk_prod_rt['rt'].T.squeeze().rename('운용수익률'), Rt_dm_stk_prod_rt['bm'].T.squeeze().rename('BM수익률')
                   , Rt_dm_stk_prod_rt['rt'].sub(Rt_dm_stk_prod_rt['bm']).T.squeeze().rename('초과수익률')
                   , Rt_dm_stk_ret_vol['rt'].T.squeeze().rename('운용변동성'), Rt_dm_stk_ret_vol['bm'].T.squeeze().rename('BM변동성')
                   , Rt_dm_stk_trek_error.T.squeeze().rename('TE'), Rt_dm_stk_shape['rt_shp'].T.squeeze().rename('운용샤프')
                   , Rt_dm_stk_shape['bm_shp'].T.squeeze().rename('BM샤프'), Rt_dm_stk_ir.T.squeeze().rename('정보비율')
                   , Rt_dm_stk_beta['alpha'].T.squeeze().rename('알파'), Rt_dm_stk_beta['beta'].T.squeeze().rename('베타')
                   , Rt_dm_stk_tryno.T.squeeze().rename('트레이너')], axis = 1)

Rt_dm_stk_des_Step.to_csv('New_Data/Rt_dm_stk_des_Step.csv', encoding='cp949')

## 전체

Rt_Stat = rt_stat(ret=tp_all_rt, rf = rf_rt, bm=bm_rt)
Rt_prod_rt = Rt_Stat.prod_ret() ## 누적수익률
Rt_ret_vol = Rt_Stat.ret_vol() ## 변동성
Rt_trek_error = Rt_Stat.trek_error() ## TE
Rt_ir = Rt_Stat.inf_rto() ## TE
Rt_shape = Rt_Stat.sharpe_rto() ## 샤프
Rt_beta = Rt_Stat.alp_beta() ## 알파 / 베타
Rt_tryno = Rt_Stat.tryn_rto() ## 트레이너

Rt_Stat.cum_rt()['rt'].sub(Rt_Stat.cum_rt()['bm'])


Rt_des_Step = pd.concat([Rt_prod_rt['rt'].T.squeeze().rename('운용수익률'), Rt_prod_rt['bm'].T.squeeze().rename('BM수익률')
                   , Rt_prod_rt['rt'].sub(Rt_prod_rt['bm']).T.squeeze().rename('초과수익률')
                   , Rt_ret_vol['rt'].T.squeeze().rename('운용변동성'), Rt_ret_vol['bm'].T.squeeze().rename('BM변동성')
                   , Rt_trek_error.T.squeeze().rename('TE'), Rt_shape['rt_shp'].T.squeeze().rename('운용샤프')
                   , Rt_shape['bm_shp'].T.squeeze().rename('BM샤프'), Rt_ir.T.squeeze().rename('정보비율')
                   , Rt_beta['alpha'].T.squeeze().rename('알파'), Rt_beta['beta'].T.squeeze().rename('베타')
                   , Rt_tryno.T.squeeze().rename('트레이너')], axis = 1)

Rt_des_Step.to_csv('New_Data/Rt_des_Step.csv', encoding='cp949')
