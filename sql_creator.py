"""
SQL Creator
"""

import re 
import dic 
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import pandas as pd
import cx_Oracle as cxo

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

st_date = '20180101'
ed_date = '20180630'
# ast_qry_key=dic.ast_qry_key ## 절대 건들지 말 것
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
    # 두번째 자산구분명 조건식 일부 if self.mat == None else self.ast_name+'_'+self.mat
        fs_qry = f"""
        WITH FT AS ({self.fs_mat_or_not()}), SND AS (
            SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
                , 평가금액
                , 장부금액
                , {self.trsc_type_sort()} 
            FROM FT
        ) SELECT STD_DT, '{self.ast_name}' 자산구분, 전일자평가금액, 평가금액
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

class bm_rf_qry:

    def __init__(self, st_dt, ed_dt):

        self.st_dt=st_dt 
        self.ed_dt=ed_dt 

    def date_creator(self):

        # RF 쿼리 내 Nested Query 담당

        return (datetime.strptime(self.st_dt, '%Y%m%d') - timedelta(days=4)).strftime('%Y%m%d')

    def bm_query(self):

        bm_qry= f"""
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
                        WHERE DT BETWEEN '{self.st_dt}' AND '{self.ed_dt}'
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
                                WHERE TRD_DT BETWEEN '{self.st_dt}' AND '{self.ed_dt}'
                            ) A, 
                            (
                                SELECT BIGN_DT, BND_WGT, FRBN_WGT, STK_WGT, FRST_WGT
                                FROM CX_TP_BMWT
                                WHERE 1=1 
                                    AND BM_CD = 'B0305'
                                    AND BIGN_DT BETWEEN '{self.st_dt}' AND '{self.ed_dt}'
                            ) B
                            WHERE A.STD_DT = B.BIGN_DT(+)
                    ) B
                    WHERE A.STD_DT = B.STD_DT
                    ORDER BY A.STD_DT
                """
        return bm_qry

    def rf_query(self):
        
        st_dt_b3 = self.date_creator()

        rf_qry = f"""
                SELECT *
                FROM
                (
                    SELECT A.TRD_DT AS STD_DT
                            , nvl(last_value(nullif(B.AMOUNT, 0)) 
                                IGNORE NULLS OVER (ORDER BY A.TRD_DT), 0) RF
                    FROM
                    (
                        SELECT TRD_DT
                        FROM FNC_CALENDAR@D_FNDB2_UFNGDBA
                        WHERE TRD_DT BETWEEN '{st_dt_b3}' AND '{self.ed_dt}'
                    ) A, 
                    (
                        SELECT TRD_DT, AMOUNT
                        FROM FNE_ECO_DATA@D_FNDB2_UFNGDBA
                        WHERE 1=1
                            AND ECO_CD = '11.02.003.009'
                            AND TERM = 'D'
                            AND TRD_DT BETWEEN '{st_dt_b3}' AND '{self.ed_dt}'
                    ) B
                    WHERE 1=1 
                        AND A.TRD_DT = B.TRD_DT(+)
                ) A
                WHERE STD_DT BETWEEN '{self.st_dt}' AND '{self.ed_dt}'
                ORDER BY STD_DT
                """
        return rf_qry

class sql_loader: 

    def __init__(self, sql):

        self.conn=dic.conn
        self.sql=sql

    def bm_read_sql(self):
        
        dt = pd.read_sql(con=self.conn,sql=self.sql).fillna(1)
        dt['STD_DT'] = pd.to_datetime(dt['STD_DT'])

        return dt.set_index('STD_DT')

    def ast_read_sql(self):
 
        dt = pd.read_sql(con=self.conn,sql=self.sql)
        dt['STD_DT'] = pd.to_datetime(dt['STD_DT'])

        return dt.set_index('STD_DT')

class sql_factory:
## 상위자산군 
# '국내채권직접', '채권_금융상품', '해외채권직접', '해외채권간접', '국내주식직접', '국내주식간접', '해외주식간접', '현금성', '국내대체직접', '국내대체간접', '해외대체간접'
    def __init__(self):
        self.ast_qry_key=dic.ast_qry_key

    def up_asset_dict(self):

        query={}
        # asset_keys=['국내채권직접', '채권_금융상품', '해외채권직접', '해외채권간접', '국내주식직접', '국내주식간접', '해외주식간접', '현금성', '국내대체직접']
        for key in self.ast_qry_key:
            if key not in ['국내대체간접', '해외대체간접']:
                query[key]=ast_sql(f'{key}'
                                            , st_date, ed_date
                                            , fund_cd=self.ast_qry_key[key]['펀드코드']
                                            , trsc_tp_cd=self.ast_qry_key[key]['거래코드']
                                            , trsc_tp_nm=self.ast_qry_key[key]['거래명']
                                            , trsc_type=self.ast_qry_key[key]['거래구분']).fs_ast_qry()
            
            elif key == '국내대체간접':
                query[key]=ast_sql(f'{key}'
                                            , st_date, ed_date
                                            , fund_cd=self.ast_qry_key[key]['펀드코드']
                                            , trsc_tp_cd=self.ast_qry_key[key]['거래코드']
                                            , trsc_tp_nm=self.ast_qry_key[key]['거래명']
                                            , trsc_type=self.ast_qry_key[key]['거래구분']
                                            , clas_code= ["'AI130'", "'AI230'", "'AI330'", "'AI360'", "'AI430'"]).fs_ast_qry()
                
            elif key == '해외대체간접':
                query[key]=ast_sql(f'{key}'
                                            , st_date, ed_date
                                            , fund_cd=self.ast_qry_key[key]['펀드코드']
                                            , trsc_tp_cd=self.ast_qry_key[key]['거래코드']
                                            , trsc_tp_nm=self.ast_qry_key[key]['거래명']
                                            , trsc_type=self.ast_qry_key[key]['거래구분']
                                            , clas_code= ["'AI140'","'AI240'","'AI340'","'AI640'", "'AI140'","'AI240'","'AI340'","'AI640'"]).fs_ast_qry()
        
        return query

                ## 세부자산별        
    def asset_dict(self, asset, name, code):## 위에 Global Scope Dictionary 를 통해 전역화 ('국내채권직접' 등의 함수를 이용하쟈)

        result = {}
        for el, clas_code in zip(name, code):
            result[f'{asset}_{el}'] = ast_sql(f'{asset}_{el}'
                                        , st_date, ed_date
                                        , fund_cd=self.ast_qry_key[asset]['펀드코드']
                                        , trsc_tp_cd=self.ast_qry_key[asset]['거래코드']
                                        , trsc_tp_nm=self.ast_qry_key[asset]['거래명']
                                        , trsc_type=self.ast_qry_key[asset]['거래구분']
                                        , clas_code=clas_code).fs_ast_qry()
        return result

    def dm_bd_mat_dict(self,mats): ## 채권_만기별

        result = {}
        for mat in mats:
            result[f'국내채권직접_{mat}'] = ast_sql(f'국내채권직접_{mat}'
                                        , st_date, ed_date
                                        , fund_cd=self.ast_qry_key['국내채권직접']['펀드코드']
                                        , trsc_tp_cd=self.ast_qry_key['국내채권직접']['거래코드']
                                        , trsc_tp_nm=self.ast_qry_key['국내채권직접']['거래명']
                                        , trsc_type=self.ast_qry_key['국내채권직접']['거래구분']
                                        , mat=mat).fs_ast_qry()
        return result
        
    def dm_bd_mat_sub_dict(self,name,code,mats): ## 채권_세부자산_만기별 

        result = {}
        for el, clas_code in zip(name, code):
            for mat in mats:
                result[f'국내채권직접_{el}_{mat}'] = ast_sql(f'국내채권직접_{el}_{mat}'
                                            , st_date, ed_date
                                            , fund_cd=self.ast_qry_key['국내채권직접']['펀드코드']
                                            , trsc_tp_cd=self.ast_qry_key['국내채권직접']['거래코드']
                                            , trsc_tp_nm=self.ast_qry_key['국내채권직접']['거래명']
                                            , trsc_type=self.ast_qry_key['국내채권직접']['거래구분']
                                            , clas_code=clas_code
                                            , mat=mat).fs_ast_qry()    
        return result

def sql_steamroller(sql_dict):
    # Get SQL Dictionary then Quearying SQL, save DataFrames in the Dictionary.

    dt={}
    result={}
    for key, value in sql_dict.items():
        dt[key]=sql_loader(value).ast_read_sql()


    for dict_key in ["수익률","장부금액","평가금액","전일자평가금액","매수","매도","별도","기초금액","기말금액"]:
        data=pd.DataFrame(index=pd.date_range(st_date,ed_date))
        for key in dt.keys():
            data[key]=dt[key][dict_key]
        result[dict_key]= data if dict_key == '수익률' else data .fillna(0)

    return result

sql_f=sql_factory()

upper_asset_qry_dict=sql_f.up_asset_dict()

## 국내채권 세부
dm_bd_sub_qry_dict=sql_f.asset_dict(asset='국내채권직접'
                            , name=["국고","금융","특수","회사"]
                            , code=["'BN110'", "'BN120'", "'BN130'", ["'BN140'", "'ST150'"]])


## 국내채권 만기
dm_bd_mat_qry_dict=sql_f.dm_bd_mat_dict(mats=["6개월미만", "6개월-1년","1년-2년","2년-3년"
                                        ,"3년-5년","5년-10년","10년-20년","20년이상"]) ## 직접 내 만기 구분

## 국내채권 세부/만기
dm_bd_sub_mat_qry_dict=sql_f.dm_bd_mat_sub_dict(name=["국고","금융","특수","회사"],
                                        code=["'BN110'", "'BN120'", "'BN130'", ["'BN140'", "'ST150'"]], 
                                        mats=["6개월미만", "6개월-1년","1년-2년","2년-3년","3년-5년","5년-10년","10년-20년","20년이상"]) ## 세부 내 만기구분

# 국내주식간접 세부
dm_stk_sub_qry_dict=sql_f.asset_dict(asset='국내주식간접', 
                            name=["성장","인덱스", "중소형주", "사회책임형", "배당형", "가치형", "액티브퀀트"], 
                            code=["'OS221'", "'OS222'", "'OS223'", "'OS224'", "'OS225'", "'OS226'", "'OS227'"])
                                        
# 해외주식 세부
ov_stk_sub_qry_dict=sql_f.asset_dict(asset='해외주식간접', 
                            name=["액티브","패시브"], 
                            code=["'OS323'", "'OS324'"])

# 국내대체직접 세부
dm_ai_j_sub_qry_dict=sql_f.asset_dict(asset='국내대체직접', 
                                name=["SOC","부동산"], 
                                code=["'AI110'", "'AI210'"])
                                
# 국내대체간접 세부
dm_ai_g_sub_qry_dict=sql_f.asset_dict(asset='국내대체간접', 
                            name=["SOC","부동산","PEF", "기타"], 
                            code=["'AI130'", "'AI230'", "'AI330'", ["'AI360'", "'AI430'"]])

# 해외대체간접 세부
ov_ai_g_sub_qry_dict=sql_f.asset_dict(asset='해외대체간접', 
                                name=["SOC","부동산","PEF", "헤지펀드"], 
                                code=["'AI140'","'AI240'","'AI340'","'AI640'"])


## And Render Data in the Excel

"""
Wait! / 해외채권간접/해외주식간접 펀드별도 해야겠네!
"""


upper_asset_data_dict=sql_steamroller(upper_asset_qry_dict)

for el in ["장부금액","평가금액","전일자평가금액","매수","매도","별도","기초금액","기말금액"]:

    upper_asset_data_dict[el]['국내채권계']=upper_asset_data_dict[el]['국내채권직접']+upper_asset_data_dict[el]['채권_금융상품']
    upper_asset_data_dict[el]['해외채권계']=upper_asset_data_dict[el]['해외채권직접']+upper_asset_data_dict[el]['해외채권간접']
    upper_asset_data_dict[el]['채권계']=upper_asset_data_dict[el]['국내채권계']+upper_asset_data_dict[el]['해외채권계']
    upper_asset_data_dict[el]['국내주식계']=upper_asset_data_dict[el]['국내주식직접']+upper_asset_data_dict[el]['국내주식간접']
    upper_asset_data_dict[el]['주식계']=upper_asset_data_dict[el]['해외주식간접']+upper_asset_data_dict[el]['국내주식계']
    upper_asset_data_dict[el]['금융자산계']=upper_asset_data_dict[el]['채권계']+upper_asset_data_dict[el]['주식계']+upper_asset_data_dict[el]['현금성']
    upper_asset_data_dict[el]['국내대체']=upper_asset_data_dict[el]['국내대체직접']+upper_asset_data_dict[el]['국내대체간접']
    upper_asset_data_dict[el]['대체전체']=upper_asset_data_dict[el]['국내대체']+upper_asset_data_dict[el]['해외대체간접']
    upper_asset_data_dict[el]['기금전체']=upper_asset_data_dict[el]['금융자산계']+upper_asset_data_dict[el]['대체전체']

for el in ['국내채권계', '해외채권계', '채권계', '국내주식계', '주식계', '금융자산계', '국내대체', '대체전체', '기금전체']:

    upper_asset_data_dict['수익률'][el]=upper_asset_data_dict['기말금액'][el].div(upper_asset_data_dict['기초금액'][el])

print(upper_asset_data_dict)



# dm_bd_sub_data_dict=sql_steamroller(dm_bd_sub_qry_dict)
# dm_bd_mat_data_dict=sql_steamroller(dm_bd_mat_qry_dict)
# dm_bd_sub_mat_data_dict=sql_steamroller(dm_bd_sub_mat_qry_dict)                                          
# dm_stk_sub_data_dict=sql_steamroller(dm_stk_sub_qry_dict)                               
# ov_stk_sub_data_dict=sql_steamroller(ov_stk_sub_qry_dict)                                
# dm_ai_j_sub_data_dict=sql_steamroller(dm_ai_j_sub_qry_dict)
# dm_ai_g_sub_data_dict=sql_steamroller(dm_ai_g_sub_qry_dict)
# ov_ai_g_sub_data_dict=sql_steamroller(ov_ai_g_sub_qry_dict)

# bm_qry=bm_rf_qry(st_dt=st_date, ed_dt=ed_date).bm_query()
# rf_qry=bm_rf_qry(st_dt=st_date, ed_dt=ed_date).rf_query()

# 하드코딩... 필요한가


## 자산데이터 Dictionary 룹 돌린 후
## 각 Element 별 데이터를 Sum해준뒤
## 기말 / 기초 수익률 산출




    ## 국내채권: 국내채권직접 / 채권
    ## 국내채권:







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





