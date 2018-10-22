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

## ast_sql에 다 집어넣을까?
## 그러면 수정해야 될 부분만 넣어서 

class ast_sql:

    def __init__(self, ast_name ,st_dt, ed_dt, fund_cd
                 , trsc_tp_cd, trsc_tp_nm, trsc_type, clas_code = None, mat=None):

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

        return ','.join(self.clas_code)

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

## 매도, 매수, 별도
## 매도 + 별도
## 매수 - 매도
## 
## 해외대체간접 쿼리가 다르네

# class sub_ast_sql(ast_sql):

#     def __init__(self, ast_name, st_dt, ed_dt, fund_cd, trsc_tp_cd=None, trsc_tp_nm=None, trsc_type=None, clsa=None):
#         super().__init__(ast_name, st_dt, ed_dt, fund_cd, trsc_tp_cd, trsc_tp_nm, trsc_type)
#         self.clsa = clsa
        
#     def test(self):

#         return self.trsc_cd_nm()


    
#     def clas_creator(self):
        
#         if bool(re.search("\국내채권직접", self.ast_name)) | bool(re.search("\국내대체직접", self.ast_name)):
#             return f'CLAS_10_CD IN ({self.clsa})'
#         elif bool(re.search("\국내주식간접", self.ast_name)):
#             return f'CLAS_11_CD IN ({self.clsa})'
#         elif bool(re.search("\해외주식간접", self.ast_name)):
#             return f'CLAS_28_CD IN ({self.clsa})'            
#         elif bool(re.search("\해외주식", self.ast_name)):
#             return f'CLAS_28_CD IN ({self.clsa})'                    
#         elif bool(re.search("\해외주식", self.ast_name)):
#             return f'CLAS_28_CD IN ({self.clsa})'                    

#     def sub_qry(self):

#         fs_qry = f"""
#                 WITH FT AS (
#                 SELECT A.STD_DT
#                     , A.평가금액
#                     , A.장부금액
#                     , {self.trsc_cd_nm()['nvl_qry']}
#                 FROM 
#                 (
#                 SELECT     STD_DT,
#                                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액,
#                                     SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
#                         FROM      CX_TP_ACNT_BY_PSBD
#                         WHERE     1=1 
#                                   AND FUND_CD IN ({self.map_fund_cd()})
#                                   AND CLAS_10_CD IN ('{self.cls_10}')                     
#                                   AND STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
#                         GROUP BY  STD_DT
#                 ) A,
#                 (
#                         SELECT    A.STD_DT
#                                   , {self.trsc_cd_nm()['sum_qry']} 
#                         FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B
#                         WHERE     A.FUND_CD IN ({self.map_fund_cd()})
#                         AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
#                         AND       A.TRSC_TP_CD IN ({self.trsc_cd_nm()['trsc_lst']})
#                         AND       A.CLAS_10_CD IN ('{self.cls_10}')                     
#                         AND       A.STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
#                         GROUP BY  A.STD_DT
#                         ORDER BY  A.STD_DT           
#                 ) B
#                 WHERE 1=1
#                 AND   A.STD_DT = B.STD_DT(+)
#                 ORDER BY A.STD_DT), SND AS (
#                 -- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
#                 SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
#                     , 평가금액
#                     , 장부금액
#                     , {self.trsc_type_sort()} 
#                 FROM FT
#                 ) SELECT STD_DT, '국내채권직접_국고' 자산구분, 전일자평가금액, 평가금액
#                         , 장부금액, 매도, 매수, 별도
#                         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
#                                 평가금액 + 매도 + 별도 
#                                 ELSE 평가금액 + 별도 END 기말금액
#                         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
#                                 전일자평가금액 + 매수 
#                                 ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
#                         , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
#                                 (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
#                                 ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
#                     FROM SND
#                     WHERE STD_DT BETWEEN {self.date_list()['st_dt']} AND {self.date_list()['ed_dt']}
#         """

#         return fs_qry

                                    

#     def sub_qry_mat(self, mat):
        
#         fs_qry = f"""WITH FT AS (SELECT A.STD_DT 
#                         , SUM(A.평가금액) 평가금액
#                         , SUM(A.장부금액) 장부금액       
#                         , {self.trsc_cd_nm()['sum_qry_sub']}     
#                     FROM (
#                             SELECT A.STD_DT
#                                 , A.ITMS_CD
#                                 , A.평가금액
#                                 , A.장부금액
#                                 , {self.trsc_cd_nm()['nvl_qry']}
#                             FROM 
#                             (
#                             SELECT     STD_DT
#                                                 , ITMS_CD
#                                                 , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE EVL_AMT END) AS 평가금액
#                                                 , SUM(CASE WHEN PVL_QTY = 0 THEN 0 ELSE ACBK_AMT END) AS 장부금액
#                                     FROM      CX_TP_ACNT_BY_PSBD
#                                     WHERE     1=1 
#                                             AND FUND_CD IN ({self.map_fund_cd()})
#                                             AND CLAS_10_CD IN ('{self.cls_10}')                     
#                                             AND STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
#                                     GROUP BY  STD_DT, ITMS_CD
#                             ) A,
#                             (
#                                     SELECT    A.STD_DT
#                                                 ,ITMS_CD
#                                                 , {self.trsc_cd_nm()['sum_qry']} 
#                                     FROM      CX_TP_ACNT_BY_TRBD A, CX_TP_BO_TRSC_TP B                   
#                                     WHERE     A.FUND_CD IN ({self.map_fund_cd()})
#                                     AND       CLAS_10_CD IN ('{self.cls_10}')
#                                     AND       A.TRSC_TP_CD = B.TRSC_TP_CD(+)
#                                     AND       A.TRSC_TP_CD IN ({self.trsc_cd_nm()['trsc_lst']})
#                                     AND       A.STD_DT BETWEEN {self.date_list()['st_dt_1']} AND {self.date_list()['ed_dt']}
#                                     GROUP BY  STD_DT, ITMS_CD
#                                     ORDER BY  A.STD_DT           
#                             ) B,
#                             (
#                                 SELECT ITMS_CD, EXPR_DT
#                                 FROM CX_TP_ASET_BAS_INF
#                             ) C
#                                 WHERE 1=1
#                                     AND A.STD_DT = B.STD_DT(+)
#                                     AND A.ITMS_CD = B.ITMS_CD(+)
#                                     AND A.ITMS_CD = C.ITMS_CD(+)
#                                     AND C.EXPR_DT {self.sub_mat(mat)}
#                                     ORDER BY A.STD_DT
#                     ) A
#                     GROUP BY A.STD_DT
#                     ORDER BY A.STD_DT), SND AS (
#                     -- SQL 매도 / 매수 / 별도 RE FUNCTION으로 변경
#                     SELECT STD_DT, LAG(평가금액) OVER (ORDER BY STD_DT) 전일자평가금액
#                         , 평가금액
#                         , 장부금액
#                         , {self.trsc_type_sort()}
#                     FROM FT
#                     ) SELECT STD_DT, '{mat}' 자산구분, 전일자평가금액, 평가금액
#                             , 장부금액, 매도, 매수, 별도
#                             , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
#                                     평가금액 + 매도 + 별도 
#                                     ELSE 평가금액 + 별도 END 기말금액
#                             , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
#                                     전일자평가금액 + 매수 
#                                     ELSE 전일자평가금액 + 매수 - 매도 END 기초금액
#                             , CASE WHEN 평가금액 = 0 AND 장부금액 = 0 THEN
#                                     (평가금액 + 매도 + 별도) / (전일자평가금액 + 매수)  
#                                     ELSE (평가금액 + 별도) / (전일자평가금액 + 매수 - 매도) END 수익률
#                         FROM SND
#                         WHERE STD_DT BETWEEN {self.date_list()['st_dt']} AND {self.date_list()['ed_dt']}
#         """

#         return fs_qry






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


## If main name 붙이기

st_date = '20180101'
ed_date = '20180630'

## Save file into Dictionary

    ## 매도, 별도 Python
    ## 매도, 별도 Python    
## 국내채권

"""
    Common: 펀드코드 / 거래코드 / 거래명 / 거래구분
    자산군 위 공통 / 세부, 세부 Inherit from 공통
    {{"펀드코드": [~], "거래코드":[], []}}
    매수 / 매도 / 변경이 자유롭지 못하다는 단점. 
    Work-flow 개선을 통해 해결? 
    - 토드로 확인 후 변경? 뭐 일단...
    Sub-Category: 채권구분 / 만기구분 / 채권/만기구분

    함수: Dictionary에 있는 변수를 쿼리 생성 함수에 집어넣는 걸로 ㄱㄱㅆ
"""

## 국내채권
    # 직접
    # key: 펀드코드 / 거래코드 /
## 국내주식
    # 직접
    # 간접    
## 해외주식(TE 주간 사용)
    # 간접
## 해외채권
    # 간접
## 국내대체
    # 직접
    # 간접
## 해외대체
    # 직접
    # 간접

# class ast_keys:

#     def __init__(self):
        
#         self.dict={}

#     def up_ast_dict(self, fund_code, trans_code, trans_name, trans_cat):
#         raise NotImplementedError

## 상반기 / 연간 성과평가
## 법인세 상정여부 
## 전체 자산에 대한 코드
ast_qry_key = {"국내채권직접": {"펀드코드": ["'111010'", "'111020'"], 
                 "거래코드": ["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"], 
                 "거래명": ['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령'],
                 "거래구분": ['매도', '매도', '매도','매수','매수','별도','별도']}, 
"채권_금융상품": {"펀드코드": ["'314010'", "'315010'"], 
                 "거래코드": ["'M4600'", "'M1300'"], 
                 "거래명": ['장기금융상품상환', '장기금융상품매수'],
                 "거래구분": ['매도', '매수']},
"해외채권직접": {"펀드코드": ["'111110'", "'111120'"], 
                 "거래코드": ["'F9100'", "'B4310'"], 
                 "거래명": ['해외FORWARD결산', '채권원리금상환'],
                 "거래구분": ['매도', '매도']},
"해외채권간접": {"펀드코드": ["'411110'"], 
                 "거래코드": ["'O4400'", "'O4300'"], 
                 "거래명": ['간접상품추가투자', '간접상품재투자'],
                 "거래구분": ['매수', '별도']},   
## 국내주식직접 변화 시 주시                               
"국내주식직접": {"펀드코드": ["'212000'", "'213000'"], 
                 "거래코드": ["'S1110'", "'S1100'", "'S4330'", "'S4310", "'S4380"], 
                 "거래명": ['주식매도', '단기매매주식매수', '유무상단주대금수령', '현금배당금확정', '배당단주대금수령'],
                 "거래구분": ['매도', '매수', '별도', '별도', '별도']},                                  
"국내주식간접": {"펀드코드": ["'412010'"], 
                 "거래코드": ["'O1410'", "'O1110'", "'O4400'", "'O1400'", "'O1100'", "'O3200'", "'O4200'"], 
                 "거래명": ['간접상품현물이체', '간접상품상환', '간접상품추가투자', '간접상품현물인수', '간접상품매수', '간접상품재투자', '간접상품현금분배금수령'],
                 "거래구분": ['매도', '매도', '매수', '매수', '매수', '별도', '별도']},                                                   
"해외주식간접": {"펀드코드": ["'412010'"], 
                 "거래코드": ["'O1100'","'O4200'","'O4300'","'O4400'"], 
                 "거래명": ['간접상품매수', '간접상품추가투자', '간접상품재투자', '간접상품현금분배금수령'],
                 "거래구분": ['매수', '매수', '매도', '매도']},                 
## 국내대체간접 AI130, AI230,AI330,AI360,AI430,AI630                  
}
## 세부자산에 대한 코드 ex)국고채 / 특수채 등

## ast_name ,st_dt, ed_dt, fund_cd, trsc_tp_cd, trsc_tp_nm, trsc_type, clas_code = None, mat=None

dm_bd_j_q   = ast_sql("'국내채권직접'"
            , st_date, ed_date
            , fund_cd=["'111010'", "'111020'"]
            , trsc_tp_cd=["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"]
            , trsc_tp_nm=['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령']
            , trsc_type=['매도', '매도', '매도','매수','매수','별도','별도']
            , )

dm_bd_j_mat   = ast_sql('국내채권직접_국고'
            , st_date, ed_date
            , fund_cd=["'111010'", "'111020'"]
            , trsc_tp_cd=["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"]
            , trsc_tp_nm=['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령']
            , trsc_type=['매도', '매도', '매도','매수','매수','별도','별도']
            , clas_code= ["'BN110'"]
            , mat='1년-2년')

print(dm_bd_j_mat.fs_ast_qry())

# dm_bd_fn   = ast_sql("'채권_금융상품'"
#             , st_date, ed_date
#             , ["'314010'", "'315010'"]
#             , ["'M4600'", "'M1300'"]
#             , ['장기금융상품상환', '장기금융상품매수']
#             , ['매도', '매수'])            

# dm_bd_j_kor = sub_ast_sql("'국내채권간접_국고'"
#             , st_date, ed_date
#             , ["'111010'", "'111020'"]
#             , ["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"]
#             , ['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령']
#             , ['매도', '매도', '매도','매수','매수','별도','별도'], 
#             'BN110')

# dm_bd_j_kor = sub_ast_sql("'국내채권간접_국고'"
#             , st_date, ed_date
#             , ["'111010'", "'111020'"]
#             , ["'B4310'", "'S1110'", "'B1110'","'B1100'","'B1350'","'B4200'","'S3020'"]
#             , ['채권원리금상환', '주식매도', '채권매도', '채권매수', '채권수요예측', '채권이자수령', '주식매도대금수령']
#             , ['매도', '매도', '매도','매수','매수','별도','별도'], 
#             'BN110')            
 
# # print(dm_bd_j_kor.sub_qry_mat("20년이상"))







        
