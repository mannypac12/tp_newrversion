import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen

year=input("Which year? ")

def url_creator(year): 
    return f"http://www.tp.or.kr:8088/tp/tpinfo/tpinfo_0104_12_tab2_2.jsp?actionType=&year={year}&month=&selectYear={year}"

url = url_creator(year)
content = urlopen(url)
soup = BeautifulSoup(content, 'html.parser')

## 1단계
cont_1 = (soup.tbody).find_all('tr')
len_1 = len(cont_1)

dic ={"운용":[], "BM":[], "초과":[]}

for i in range(len_1):

    nestd_arr = []
    nestd_td=cont_1[i].find_all('td')
    len_nested=len(nestd_td)

    for j in range(len_nested):
        nestd_arr.append(nestd_td[j].string)
    if (i) % 3 == 0: 
        dic["운용"].append(nestd_arr)
    elif (i) % 3 == 1: 
        dic["BM"].append(nestd_arr)
    elif (i) % 3 == 2:         
        dic["초과"].append(nestd_arr)
    

columns = ["국내채권직접", "국내채권간접", "해외채권직접", "해외채권간접", "금융상품", "국내주식직접", "국내주식간접", "해외주식간접", "대체투자", "현금성", "계"]

ast_rt_crwl=pd.DataFrame(dic["운용"], index = [f"{i}월" for i in range(len_1//3,0,-1)], columns=columns)
bm_rt_crwl=pd.DataFrame(dic["BM"], index = [f"{i}월" for i in range(len_1//3,0,-1)], columns=columns)
ex_rt_crwl=pd.DataFrame(dic["초과"], index = [f"{i}월" for i in range(len_1//3,0,-1)], columns=columns)

ast_rt_crwl.to_csv("crawling_dt/ast_rt_crwl.csv", encoding='cp949')
bm_rt_crwl.to_csv("crawling_dt/bm_rt_crwl.csv", encoding='cp949')
ex_rt_crwl.to_csv("crawling_dt/ex_rt_crwl.csv", encoding='cp949')



