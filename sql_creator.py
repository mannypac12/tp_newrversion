"""
SQL Creator
"""

import re
from datetime import datetime
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

from datetime import datetime, timedelta

class ast_sql:

    def __init__(self, ast_name ,st_dt, ed_dt, fund_cd, trsc_tp_cd = None, trsc_tp_nm = None, trsc_type= None):

        self.ast_name = ast_name
        self.st_dt = st_dt
        self.ed_dt = ed_dt
        self.fund_cd  = fund_cd
        self.trsc_tp_cd = trsc_tp_cd
        self.trsc_tp_nm = trsc_tp_nm
        self.trsc_type = trsc_type
    
    def date_list(self):

        # 날짜 변수를 관리하는 부분 - List 0

        st_dt_1 = (datetime.strptime(self.st_dt, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

        return {"st_dt": self.st_dt, "ed_dt": self.ed_dt, "st_dt_1": st_dt_1}

    def map_fund_cd(self):
        
        # fund_cd 를 관리하하는 부분 - list to string

        return ','.join(self.fund_cd)

    ## 매도 / 매수 / 별도 Dictionary 를 만들 것

    def trsc_cd_nm(self):

        sum_qry = ','.join(list(map(lambda x,y:f'SUM(CASE WHEN A.TRSC_TP_CD = {x} THEN A.STTL_AMT END) AS {y}', self.trsc_tp_cd, self.trsc_tp_nm)))
        nvl_qry = ','.join(list(map(lambda x:f'NVL(B.{x}, 0) AS {x}', self.trsc_tp_nm)))
        sum_qry_sub = ','.join(list(map(lambda x:f'SUM(A.{x}) AS {x}', self.trsc_tp_nm)))
        trsc_lst = ','.join(self.trsc_tp_cd)

        return {'sum_qry': sum_qry, 'nvl_qry': nvl_qry, 'trsc_lst': trsc_lst, 'sum_qry_sub': sum_qry_sub}

    def trsc_type_sort(self):

        ## 매도 매수 별도 목록 등을 Dictionary 로 만든 후
        ## 쿼리 형으로 변환
        ## 매도 매수 별도 중 들어가지 않은 항목이 있을 경우 이를 모두 고려할 수 있음.

        result = {}

        for key, value in zip(self.trsc_type, self.trsc_tp_nm):
            result.setdefault(key, []).append(value)


        return ','.join(map(lambda x, y: '+'.join(x) + ' ' + y, list(result.values()), list(result.keys())))
        ## if 

    def fs_qry(self):

        fs_qry = f"""
        WITH FT AS (
        SELECT A.STD_DT
            , A.평가금액
            , A.장부금액
            , {self.trsc_cd_nm()['nvl_qry']}
        FROM 
        (
               SELECT     STD_DT,
                         SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                         SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
               FROM      CX_TP_ACNT_BY_PSBD
               WHERE     1=1 
                         AND FUND_CD IN ({self.map_fund_cd()})
                         AND STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
               GROUP BY  STD_DT
        ) A,
        (
               SELECT    A.STD_DT
                        , {self.trsc_cd_nm()['sum_qry']} 
               FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
               WHERE     A.FUND_CD IN ({self.map_fund_cd()})
               AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
               AND       A.TRSC_TP_CD IN ({self.trsc_cd_nm()['trsc_lst']})
               AND       A.STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
               GROUP BY  A.STD_DT
               ORDER BY  A.STD_DT
        ) B
        WHERE 1=1
            AND   A.STD_DT = B.STD_DT(+)
            ORDER BY A.STD_DT), SND AS (
            SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
                , 평가금액
                , 장부금액
                , {self.trsc_type_sort()} 
            FROM FT
        ) SELECT STD_DT, {self.ast_name} 자산구분, 전일자평가금액, 평가금액
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


class sub_ast_sql(ast_sql):

    def __init__(self, ast_name, st_dt, ed_dt, fund_cd, trsc_tp_cd=None, trsc_tp_nm=None, trsc_type=None, cls_10=None):
        super().__init__(ast_name, st_dt, ed_dt, fund_cd, trsc_tp_cd, trsc_tp_nm, trsc_type)
        self.cls_10 = cls_10
        
    def test(self):

        return self.trsc_cd_nm()

    def sub_qry(self):

        fs_qry = f"""
                WITH FT AS (
                SELECT A.STD_DT
                    , A.평가금액
                    , A.장부금액
                    , {self.trsc_cd_nm()['nvl_qry']}
                FROM 
                (
                SELECT     STD_DT,
                                    SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
                                    SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                        FROM      CX_TP_ACNT_BY_PSBD
                        WHERE     1=1 
                                  AND FUND_CD IN ({self.map_fund_cd()})
                                  AND CLAS_10_CD IN ('{self.cls_10}')                     
                                  AND STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
                        GROUP BY  STD_DT
                ) A,
                (
                        SELECT    A.STD_DT
                                  , {self.trsc_cd_nm()['sum_qry']} 
                        FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
                        WHERE     A.FUND_CD IN ({self.map_fund_cd()})
                        AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
                        AND       A.TRSC_TP_CD IN ({self.trsc_cd_nm()['trsc_lst']})
                        AND       A.CLAS_10_CD IN ('{self.cls_10}')                     
                        AND       A.STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
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
                    , {self.trsc_type_sort()} 
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
                    WHERE STD_DT BETWEEN {self.date_list()['st_dt']} AND {self.date_list()['ed_dt']}
        """

        return fs_qry

    def sub_mat(self, mat):

        ## 만기 구분 바뀌면 반영해서 바꿔줄 것
        
        if mat == "6개월미만":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=0)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=6)- relativedelta(days=1)).strftime('%Y%m%d')

            return f"""BETWEEN {former} AND {letter}"""
        
        elif mat == "6개월-1년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=6)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=12)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""BETWEEN {former} AND {letter}"""
            
        elif mat == "1년-2년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=12)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=24)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""BETWEEN {former} AND {letter}"""

        elif mat == "2년-3년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=24)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=36)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""BETWEEN {former} AND {letter}"""

        elif mat == "3년-5년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=36)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=60)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""BETWEEN {former} AND {letter}"""

        elif mat == "5년-10년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=60)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=120)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""BETWEEN {former} AND {letter}"""

        elif mat == "10년-20년":

            former = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=120)).strftime('%Y%m%d')
            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=240)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f"""BETWEEN {former} AND {letter}"""

        elif mat == "20년이상":

            letter = (datetime.strptime(self.ed_dt, '%Y%m%d') + relativedelta(months=240)- relativedelta(days=1)).strftime('%Y%m%d')            

            return f""">{letter}"""                                    

    def sub_qry_mat(self, mat):
        
        fs_qry = f"""WITH FT AS (SELECT A.STD_DT 
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
                            (
                            SELECT     STD_DT
                                                , ITMS_CD
                                                , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
                                                , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
                                    FROM      CX_TP_ACNT_BY_PSBD
                                    WHERE     1=1 
                                            AND FUND_CD IN ({self.map_fund_cd()})
                                            AND CLAS_10_CD IN ('{self.cls_10}')                     
                                            AND STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
                                    GROUP BY  STD_DT, ITMS_CD
                            ) A,
                            (
                                    SELECT    A.STD_DT
                                                ,ITMS_CD
                                                , {self.trsc_cd_nm()['sum_qry']} 
                                    FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B                   
                                    WHERE     A.FUND_CD IN ({self.map_fund_cd()})
                                    AND       CLAS_10_CD IN ('{self.cls_10}')
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
                                    AND A.ITMS_CD = C.ITMS_CD(+)
                                    AND C.EXPR_DT {self.sub_mat(mat)}
                                    ORDER BY A.STD_DT
                    ) A
                    GROUP BY A.STD_DT
                    ORDER BY A.STD_DT), SND AS (
                    -- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
                    SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
                        , 평가금액
                        , 장부금액
                        , {self.trsc_type_sort()}
                    FROM FT
                    ) SELECT STD_DT, '{mat}' 자산구분, 전일자평가금액, 평가금액
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






        ## mat: Month 기준            
        ## mat: Month 기준                    

        # 6개월 미만
        # 6월 이상 ~ 1년 미만
        # 1년 ~ 2년
        # 2년 ~ 3년
        # 3년 ~ 5년
        # 5년 ~ 10년
        # 10년 ~ 20년        
        # 20년 이상




st_date = '20180101'
ed_date = '20180630'


dm_bd_j_q   = ast_sql("'국내채권간접'"
            , st_date, ed_date
            , ["'111010'", "'111020'"]
            , ["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"]
            , ['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령']
            , ['매도', '매도', '매도','매수','매수','별도','별도'])

dm_bd_j_q   = ast_sql("'국내채권간접'"
            , st_date, ed_date
            , ["'111010'", "'111020'"]
            , ["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"]
            , ['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령']
            , ['매도', '매도', '매도','매수','매수','별도','별도'])

dm_bd_j_kor = sub_ast_sql("'국내채권간접_국고'"
            , st_date, ed_date
            , ["'111010'", "'111020'"]
            , ["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"]
            , ['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령']
            , ['매도', '매도', '매도','매수','매수','별도','별도'], 
            'BN110')


 

print(dm_bd_j_kor.sub_qry_mat("20년이상"))




        
