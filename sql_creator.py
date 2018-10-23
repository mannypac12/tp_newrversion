"""
SQL Creator
"""

import re 
import dic 
from datetime import datetime, timedelta
from dateutil.relativedelta import *

## SQL1
    ## Only FUND_CD Changed 
    ## AS Fund_cd changed TRSC_TP_CD also the numbers of fund_cd should be changed
    ## TRSC_TP_CD changed then other component should be chaged

"""
Assembly Line

날짜 변수를 관리하는 부분 - List 0
fund_cd 를 관리하하는 부분 - list to string
거래코드 / 거래명 관리 - list to string
거래코드 / 거래명 변형 쿼리 관리 부분 - list to string
"""



## ast_sql에 다 집어넣을까?
## 그러면 수정해야 될 부분만 넣어서 

class ast_sql:

    def __init__(self, ast_name ,st_dt, ed_dt, trsc_tp_cd,
                 trsc_tp_nm, trsc_type, fund_cd=None, clas_code=None, mat=None):

        self.ast_name = ast_name
        self.st_dt = st_dt
        self.ed_dt = ed_dt
        self.fund_cd  = fund_cd
        self.trsc_tp_cd = trsc_tp_cd
        self.trsc_tp_nm = trsc_tp_nm
        self.trsc_type = trsc_type
        self.clas_code = clas_code
        self.mat = mat
    
    def date_list(self):

        # 날짜 변수를 관리하는 부분 - List 0

        st_dt_1 = (datetime.strptime(self.st_dt, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

        return {"st_dt": self.st_dt, "ed_dt": self.ed_dt, "st_dt_1": st_dt_1}

    def trsc_cd_nm(self):

        sum_qry = ','.join(list(map(lambda x,y:f'SUM(CASE WHEN A.TRSC_TP_CD = {x} THEN A.STTL_AMT END) AS {y}', self.trsc_tp_cd, self.trsc_tp_nm)))
        nvl_qry = ','.join(list(map(lambda x:f'NVL(B.{x}, 0) AS {x}', self.trsc_tp_nm)))
        sum_qry_sub = ','.join(list(map(lambda x:f'SUM(A.{x}) AS {x}', self.trsc_tp_nm)))
        trsc_lst = ','.join(self.trsc_tp_cd)

        return {'sum_qry': sum_qry, 'nvl_qry': nvl_qry, 'trsc_lst': trsc_lst, 'sum_qry_sub': sum_qry_sub}

    ## 매도 / 매수 / 별도 Dictionary 를 만들 것

    def trsc_type_sort(self):

        ## 매도 매수 별도 목록 등을 Dictionary 로 만든 후
        ## 쿼리 형으로 변환
        ## 매도 매수 별도 중 들어가지 않은 항목이 있을 경우 이를 모두 고려할 수 있음.
    
        result = {}

        for key, value in zip(self.trsc_type, self.trsc_tp_nm):
            result.setdefault(key, []).append(value)
        
        ## 만약 매수 매도 별도 중 하나 / 혹은 그 이상이 들어가있지 않다면, 그 해당 요소를 (매수: 0) 등으로 변환시켜줘야 함.
        for el in ["매수", "매도", "별도"]:
            if el not in result:
                result[el] = '0'
            else: 
                pass
        # ','.join(map(lambda x, y: '+'.join(x) + ' ' + y, list(result.values()), list(result.keys())))
        return ','.join(map(lambda x, y: '+'.join(x) + ' ' + y, list(result.values()), list(result.keys())))

    def clas_converter(self):

        if type(self.clas_code) == list:
            return ','.join(self.clas_code)
        else: 
            return self.clas_code if self.clas_code != None else None
            
        # return ','.join(self.clas_code) if self.clas_code != None else 1

    def map_fund_cd(self):
        
        ## 해외대체간접인 경우
        ## 해외대체간접이 아닌 경우        
        if bool(re.search('\해외대체간접', self.ast_name)):
            return f"""AND CLAS_13_CD IN ('413010','621110','631110','611010')"""
        else:
            return f"""AND FUND_CD IN ({','.join(self.fund_cd)})"""

    def clsa_creator(self):
        ## 모든 SubAsset 표현식은 "상위자산군_하위자산군"으로 가정
        ## 국내대체간접일 경우 AI130, AI230, AI330, AI360, AI430, AI630
        ## IF 국내채권 CLAS_10 
        ## IF 국내주식 CLAS_11
        ## IF 해외주식 CLAS_28

        clas = self.clas_converter()

        if bool(re.search('\국내(채권직접|대체직접)_[가-힣]|(해외대체|국내대체)간접', self.ast_name)):
            return f'AND CLAS_10_CD IN ({clas})'
        elif bool(re.search('\국내주식간접_[가-힣]+', self.ast_name)):
            ## 국내주식간접_성장 등
            return f'AND CLAS_11_CD IN ({clas})'
        elif bool(re.search('\해외주식간접_[가-힣]+', self.ast_name)):
            ## 해외주식_액티브 / 해외주식_패시브
            return f'AND CLAS_28_CD IN ({clas})'
        else:
            return ""
    
    def where_qry_creator(self):
        
        return f'{self.map_fund_cd()} {self.clsa_creator()}'
        ## Where 문의 쿼리를 Sub Asset의 쿼리에 따라 바꿔주는 Function
        ## AND 로 먼저시작하게 할 것
        ## Mat expr 이 있을 경우 아래에 괄호 붙이지 말고 바로 옆에 괄호 붙일것. 만약 없으면 { } 요걸로 조져주기

    def sub_mat(self):

        ## 만기 구분 바뀌면 반영해서 바꿔줄 것
        
        if self.mat == "6개월미만":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=0)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=6)- relativedelta(days=1)).strftime('%Y%m%d')

            return f"""AND C.EXPR_DT BETWEEN {former} AND {letter}"""
        
        elif self.mat == "6개월-1년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=6)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=12)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""AND C.EXPR_DT BETWEEN {former} AND {letter}"""
            
        elif self.mat == "1년-2년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=12)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=24)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""AND C.EXPR_DT BETWEEN {former} AND {letter}"""

        elif self.mat == "2년-3년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=24)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=36)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""AND C.EXPR_DT BETWEEN {former} AND {letter}"""

        elif self.mat == "3년-5년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=36)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=60)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""AND C.EXPR_DT BETWEEN {former} AND {letter}"""

        elif self.mat == "5년-10년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=60)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=120)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""AND C.EXPR_DT BETWEEN {former} AND {letter}"""

        elif self.mat == "10년-20년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=120)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=240)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""AND C.EXPR_DT BETWEEN {former} AND {letter}"""

        elif self.mat == "20년이상":

            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=240)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""AND C.EXPR_DT>{letter}"""

        elif self.mat == None:

            return f""" """
    
    def fs_mat_or_not(self):

        if self.mat == None:
            return f"""
                    SELECT A.STD_DT
                        , A.평가금액
                        , A.장부금액
                        , {self.trsc_cd_nm()['nvl_qry']}
                    FROM 
                    (
                        SELECT    STD_DT,
                                    SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                                    SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                        FROM      CX_TP_ACNT_BY_PSBD
                        WHERE     1=1 {self.where_qry_creator()}
                                AND STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
                        GROUP BY  STD_DT
                    ) A,
                    (
                        SELECT    A.STD_DT
                                    , {self.trsc_cd_nm()['sum_qry']} 
                        FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                        WHERE     1=1
                        {self.where_qry_creator()}
                        AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                        AND       A.TRSC_TP_CD IN ({self.trsc_cd_nm()['trsc_lst']})
                        AND       A.STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
                        GROUP BY  A.STD_DT
                        ORDER BY  A.STD_DT
                    ) B
                        WHERE 1=1
                            AND   A.STD_DT = B.STD_DT(+)
                            ORDER BY A.STD_DT    
                    """
        else:            
            return f"""
            SELECT A.STD_DT 
                , SUM(A.평가금액) 평가금액
                , SUM(A.장부금액) 장부금액       
                , {self.trsc_cd_nm()['sum_qry_sub']}
            FROM (
                    SELECT A.STD_DT
                        , A.ITMS_CD
                        , A.평가금액
                        , A.장부금액
                        , {self.trsc_cd_nm()['nvl_qry']}
                    FROM 
                    (SELECT  STD_DT
                                    , ITMS_CD
                                    , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                                    , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                                FROM      CX_TP_ACNT_BY_PSBD
                                WHERE   1=1 
                                        {self.where_qry_creator()}
                                        AND STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
                                GROUP BY  STD_DT, ITMS_CD
                    ) A,
                    (
                        SELECT    A.STD_DT
                                ,ITMS_CD
                                , {self.trsc_cd_nm()['sum_qry']} 
                        FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B                   
                        WHERE     1=1
                        {self.where_qry_creator()}
                        AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                        AND       A.TRSC_TP_CD IN ({self.trsc_cd_nm()['trsc_lst']})
                        AND       A.STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
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
                                AND A.ITMS_CD = C.ITMS_CD(+) {self.sub_mat()}
                                ORDER BY A.STD_DT
                ) A
                GROUP BY A.STD_DT
                ORDER BY A.STD_DT
                    """
    def fs_ast_qry(self):

        fs_qry = f"""
        WITH FT AS ({self.fs_mat_or_not()}), SND AS (
            SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
                , 평가금액
                , 장부금액
                , {self.trsc_type_sort()} 
            FROM FT
        ) SELECT STD_DT, '{self.ast_name if self.mat == None else self.ast_name+'_'+self.mat}' 자산구분, 전일자평가금액, 평가금액
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
        WHERE STD_DT BETWEEN {self.date_list()['st_dt']} AND {self.date_list()['ed_dt']}
        """

        return fs_qry


## If main name 붙이기



## Loop 으로 Dictionary에 저장해볼까?

## 상위자산군 -- 별도의 변수로 만들어보쟈
## 하위자산군 -- 국고, 금융 특수, 회사채 / 만기별 / 특수별 만기는 할 수 있을 듯 해

 ## Global Scope

st_date = '20180101'
ed_date = '20180630'

ast_qry_key=dic.ast_qry_key

def up_asset_dict():
    
    query={}
    # asset_keys=['국내채권직접', '채권_금융상품', '해외채권직접', '해외채권간접', '국내주식직접', '국내주식간접', '해외주식간접', '현금성', '국내대체직접']
    for key in ast_qry_key:
        if key not in ['국내대체간접', '해외대체간접']:
            query[key]=ast_sql(f'{key}'
                                        , st_date, ed_date
                                        , fund_cd=ast_qry_key[key]['펀드코드']
                                        , trsc_tp_cd=ast_qry_key[key]['거래코드']
                                        , trsc_tp_nm=ast_qry_key[key]['거래명']
                                        , trsc_type=ast_qry_key[key]['거래구분']).fs_ast_qry()
        
        elif key == '국내대체간접':
            query[key]=ast_sql(f'{key}'
                                        , st_date, ed_date
                                        , fund_cd=ast_qry_key[key]['펀드코드']
                                        , trsc_tp_cd=ast_qry_key[key]['거래코드']
                                        , trsc_tp_nm=ast_qry_key[key]['거래명']
                                        , trsc_type=ast_qry_key[key]['거래구분']
                                        , clas_code= ["'AI130'", "'AI230'", "'AI330'", "'AI360'", "'AI430'"]).fs_ast_qry()
            
        elif key == '해외대체간접':
            query[key]=ast_sql(f'{key}'
                                        , st_date, ed_date
                                        , fund_cd=ast_qry_key[key]['펀드코드']
                                        , trsc_tp_cd=ast_qry_key[key]['거래코드']
                                        , trsc_tp_nm=ast_qry_key[key]['거래명']
                                        , trsc_type=ast_qry_key[key]['거래구분']
                                        , clas_code= ["'AI140'","'AI240'","'AI340'","'AI640'", "'AI140'","'AI240'","'AI340'","'AI640'"]).fs_ast_qry()
    
    return query
## 세부자산별        



def asset_dict(asset, name, code): ## 위에 Global Scope Dictionary 를 통해 전역화 ('국내채권직접' 등의 함수를 이용하쟈)

    result = {}
    for el, clas_code in zip(name, code):
        result[f'{asset}_{el}'] = ast_sql(f'{asset}_{el}'
                                    , st_date, ed_date
                                    , fund_cd=ast_qry_key[asset]['펀드코드']
                                    , trsc_tp_cd=ast_qry_key[asset]['거래코드']
                                    , trsc_tp_nm=ast_qry_key[asset]['거래명']
                                    , trsc_type=ast_qry_key[asset]['거래구분']
                                    , clas_code=clas_code).fs_ast_qry()
    return result

## 채권_만기별

def dm_bd_mat_dict(mats):

    result = {}
    for mat in mats:
        result[f'국내채권직접_{mat}'] = ast_sql(f'국내채권직접_{mat}'
                                    , st_date, ed_date
                                    , fund_cd=ast_qry_key['국내채권직접']['펀드코드']
                                    , trsc_tp_cd=ast_qry_key['국내채권직접']['거래코드']
                                    , trsc_tp_nm=ast_qry_key['국내채권직접']['거래명']
                                    , trsc_type=ast_qry_key['국내채권직접']['거래구분']
                                    , mat=mat).fs_ast_qry()
    return result
    
def dm_bd_mat_sub_dict(name,code,mats):

    result = {}
    for el, clas_code in zip(name, code):
        for mat in mats:
            result[f'국내채권직접_{el}'] = ast_sql(f'국내채권직접_{el}'
                                        , st_date, ed_date
                                        , fund_cd=ast_qry_key['국내채권직접']['펀드코드']
                                        , trsc_tp_cd=ast_qry_key['국내채권직접']['거래코드']
                                        , trsc_tp_nm=ast_qry_key['국내채권직접']['거래명']
                                        , trsc_type=ast_qry_key['국내채권직접']['거래구분']
                                        , clas_code=clas_code
                                        , mat=mat).fs_ast_qry()    
    return result


"""
Wait! / 해외채권간접/해외주식간접 펀드별도 해야겠네!
"""

## 상위자산군 
# '국내채권직접', '채권_금융상품', '해외채권직접', '해외채권간접', '국내주식직접', '국내주식간접', '해외주식간접', '현금성', '국내대체직접', '국내대체간접', '해외대체간접'
upper_asset_qry_dict=up_asset_dict()["국내대체간접"]

## 국내채권 세부
dm_bd_sub_qry_dict=asset_dict(asset='국내채권직접', name=["국고","금융","특수","회사"], code=["'BN110'", "'BN120'", "'BN130'", ["'BN140'", "'ST150'"]])
## 국내채권 세부(만기)
dm_bd_mat_qry_dict=dm_bd_mat_dict(mats=["6개월미만", "6개월-1년","1년-2년","2년-3년","3년-5년","5년-10년","10년-20년","20년이상"]) ## 직접 내 만기 구분
dm_bd_sub_mat_qry_dict=dm_bd_mat_sub_dict(name=["국고","금융","특수","회사"],
                                          code=["'BN110'", "'BN120'", "'BN130'", ["'BN140'", "'ST150'"]], 
                                          mats=["6개월미만", "6개월-1년","1년-2년","2년-3년","3년-5년","5년-10년","10년-20년","20년이상"]) ## 세부 내 만기구분
# 국내주식간접 세부
dm_stk_sub_qry_dict=asset_dict(asset='국내주식간접', 
                               name=["성장","인덱스","중소형주","사회책임형", "배당형", "가치형", "액티브퀀트형"], 
                               code=["'OS221'", "'OS222'", "'OS223'", "'OS224'", "'OS225'", "'OS226'", "'OS227'"])
# 해외주식 세부
ov_stk_sub_qry_dict=asset_dict(asset='국내주식간접', 
                               name=["액티브","패시브"], 
                               code=["'OS323'", "'OS324'"])
# 국내대체직접 세부
dm_ai_j_sub_qry_dict=asset_dict(asset='국내대체직접', 
                                name=["SOC","부동산"], 
                                code=["'AI110'", "'AI210'"])
# 국내대체간접 세부
dm_ai_g_sub_qry_dict=asset_dict(asset='국내대체간접', 
                               name=["SOC","부동산","PEF", "기타"], 
                               code=["'AI130'", "'AI230'", "'AI330'", ["'AI360'", "'AI430'"]])
# 해외대체간접 세부
ov_ai_g_sub_qry_dict=asset_dict(asset='해외대체간접', 
                                name=["SOC","부동산","PEF", "헤지펀드"], 
                                code=["'AI140'","'AI240'","'AI340'","'AI640'"])

