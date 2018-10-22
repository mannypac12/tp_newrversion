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