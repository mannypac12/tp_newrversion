import pandas as pd
import numpy as np
from statsmodels.formula.api import ols

class rt_stat:

    # 주의: BM데이터의 포맷과 RETURN 데이터의 포맷은 똑같아야함. 안그러면 망 ㅎ

    def __init__(self, ret, rf=None, bm=None):

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


## 비중 산출할 것. 

## 목표

## Return과 관련한 지표 생성 / 장부금액 비중 생성
    ## class 
    ## RF 를 바꿔줘야 함(바꿈 / 쿼리에서) / 2019년에 366으로 바꿔줄것(큰 차이는 없쥐)
    
## 성과요인분해 만들기

    ## 수익률, 기초비중
    ## 기금
        ## 상위자산군 DataFrame에서 해결할 것.
    ## 채권
        ## 상위자산군에서 가뎌오긔
    ## 주식
        ## 상위자산군에서 가뎌오긔

## 시험용

rt=pd.read_csv('Data/상위자산군_수익률.csv', engine='python',encoding='utf-8',index_col=0)
bm=pd.read_csv('Data/bm_rt.csv', engine='python',encoding='utf-8',index_col='STD_DT')
rf=pd.read_csv('Data/rf_rt.csv', engine='python',encoding='utf-8',index_col='STD_DT')
rt.index=pd.to_datetime(rt.index)
bm.index=pd.to_datetime(bm.index)
rf.index=pd.to_datetime(rf.index)

print(rt_stat(ret=rt,bm=bm,rf=rf).alp_beta())
## And Render Data in the Excel
