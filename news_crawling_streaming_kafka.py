from kafka import KafkaProducer
from json import dumps
import time

producer = KafkaProducer(acks=0,
                         compression_type='gzip',
                         bootstrap_servers=['13.209.5.85:9094'],
                         value_serializer=lambda x:dumps(x).encode('utf-8'),
                          api_version=(2,)
                         )



import logging


import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from datetime import datetime
import time
import os
from tqdm import tqdm
import re
# import mysql.connector
import random

# 로그 생성
logger = logging.getLogger()

# 로그의 출력 기준 설정
logger.setLevel(logging.INFO)

# log 출력 형식
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# log를 console에 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

"""
100 : 정치
101 : 경제
102 : 사회
103 : 생활/문화
104 : 세계
105 : IT/과학
"""


##오늘날짜얻기
today = datetime.today().strftime('%Y%m%d')
#print(today)

cats = [100, 101, 102, 103 ,104, 105]
last_url = {100:"",101:"",102:"",103:"",104:"",105:""}

##data 폴더 생성
try:
    if not os.path.exists("data"):
        os.mkdir("data")
except OSError:
    logger.info('Error: Creating directory. ' +  "data")


def cleanText(readData):
  return re.sub(" {2,}", ' ', re.sub("[\t\n\v\f\r]", '', readData))


def generate_weighted_random_number(min, max):
    # 0.5에서 1.5 사이에서 소수점 둘째 자리까지 랜덤 숫자 생성
    random_number = random.uniform(min, max)

    # 0.5가 더 자주 나오게 하기 위해 조절된 확률 사용
    if random.random() < 0.7:  # 예시로 0.5가 더 자주 나오도록 조절 (0.7은 실험에 따라 조절 가능)
        return 1.1
    else:
        if random.random() < 0.99:
            return random_number
        else:
            return 3

def make_urllist(cat, date):
    global last_url
    page_num = 0
    first_url = ""
    urllist= []
    first_flag = 0
    last_flag = 0

    for i in range(10):
        url = 'https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1='+str(cat)+'&date='+str(date)+'&page='+str(i+1)
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
        news = requests.get(url, headers=headers)

        # BeautifulSoup의 인스턴스 생성합니다. 파서는 html.parser를 사용합니다.
        soup = BeautifulSoup(news.content, 'html.parser')

        # CASE 1
        news_list = soup.select('.newsflash_body .type06_headline li dl')
        # CASE 2
        news_list.extend(soup.select('.newsflash_body .type06 li dl'))


        # 각 뉴스로부터 a 태그인 <a href ='주소'> 에서 '주소'만을 가져옵니다.
        for idx, line in enumerate(news_list):
            tmp_url = line.a.get('href')
            #바뀐페이지의 젓번째 url의 변동이없을때 탈출
            if idx == 0:
                if first_url == tmp_url:
                    first_flag = 1
                    break
                else:
                    first_url = tmp_url

            #마지막 db에 저장된 url과 비교
            if last_url[cat] == tmp_url:
                last_flag = 1
                break
            urllist.append(tmp_url)


        if first_flag == 1:
            break
        if last_flag == 1:
            break
        #뉴스 개수가 20개가 안되면 탈출
        if len(news_list) < 20:
            break

        time.sleep(generate_weighted_random_number(0.8, 1.4))

    return urllist


def send_info(cats, url_lists, today):
    global last_url
    # cat 값에 따라 url_list에 접근
    for cat in tqdm(cats):
        #print(f"cat {i}: {url_lists[i]}")

        #print(list(reversed(url_lists[cat])))
        for url in list(reversed(url_lists[cat])):
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}

            res = requests.get(url, headers=headers)
            soup = BeautifulSoup(res.text, 'lxml')


            #print(soup)
            #newslist = soup.select(".office_header")
            #print(len(newslist))

            ##뉴스업체
            #ofhd_float_title_text
            try:

                siteName = soup.find(class_="c_inner")
                siteName_val = siteName.text.split("ⓒ")[1].strip().split('.')[0].strip()
            except:
                continue
                #siteName_val = None
                #print(url)

            ##기사제목
            #media_end_head_headline
            try:
                title = soup.find(class_="media_end_head_headline")
                title_val = cleanText(title.text)
            except:
                continue
                #title_val = None
                #print(url)

            ##작성일자
            #media_end_head_info_datestamp_time _ARTICLE_DATE_TIME
            try:
                createDate = soup.find(class_="media_end_head_info_datestamp_time _ARTICLE_DATE_TIME")
                createDate_val = createDate.text
            except:
                continue
                #createDate_val = None
                #print(url)

            ##수정일자
            #media_end_head_info_datestamp_time _ARTICLE_MODIFY_DATE_TIME
            try:
                modifyDate = soup.find(class_="media_end_head_info_datestamp_time _ARTICLE_MODIFY_DATE_TIME")
                modifyDate_val = modifyDate.text
            except:
                modifyDate_val = None


            ##본문내용
            #dic_area 안 br카테고리 txt
            try:
                #text_list = ""

                brs = soup.find(id="dic_area")
                #print(len(brs))
                #brs = lis.findAll("br")
                #brs.findAll("br")

                text_list = cleanText(brs.get_text(strip=True))
                #for br in brs:
                #    text_list.append(br.get_text(strip=True))
                #print(text_list)
            except:
                print(url)


            ##이미지 URL
            #end_photo_org
            try:
                img_url_list = []

                urs = soup.findAll(class_="end_photo_org")
                #print(len(urs))
                for ur in urs:
                    img_url_list.append(ur.img['data-src'])
                #print(img_url_list)
            except:
                print(url)

            ##작성기자 기자 이메일
            #byline_s
            try:
                author_list = []

                authors = soup.findAll(class_="byline_s")
                #print(len(authors))
                for au in authors:
                    author_list.append(au.text)
                #print(author_list)
            except:
                print(url)

            # # db연결
            # db_connection = mysql.connector.connect(
            #         host="shtestdb.duckdns.org",
            #         user="eun",
            #         port="13306",
            #         password="12341234!",
            #         database="eun_test",
            #     )
            # cursor = db_connection.cursor()

            # 'news' 토픽으로 전송


            news_data = {
                    'category': cat,
                    'url': url,
                    'site_name': siteName_val,
                    'title': title_val,
                    'author_info': ' '.join(author_list),
                    'create_date': createDate_val,
                    'modify_date': modifyDate_val,
                    'content': text_list,
                    'image_url': ' '.join(img_url_list)
                }
            producer.send('news_test', value=news_data)



            if modifyDate_val != "":
                article_data = [
                    ( cat, url, siteName_val, title_val, str(' '.join(author_list)), createDate_val, modifyDate_val, text_list, str(' '.join(img_url_list))),
                    # 다른 기사 데이터도 추가
                    ]
            else:
                article_data = [
                    ( cat, url, siteName_val, title_val, str(' '.join(author_list)), createDate_val, modifyDate_val, text_list, str(' '.join(img_url_list))),
                    # 다른 기사 데이터도 추가
                    ]
            article_query = """
            INSERT IGNORE INTO Article (category_id, news_url, company_name, title, author_info, create_date, modify_date, content, image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """



            # cursor.executemany(article_query, article_data)
            # db_connection.commit()

            # # 연결 종료
            # cursor.close()
            # db_connection.close()

            last_url[cat] = url

            time.sleep(generate_weighted_random_number(0.8, 2.2))



logger.info("코드가 시작됩니다.")

while True:

    now = datetime.today().strftime('%Y%m%d')

    url_lists = {}
    count = 0

    if today == now:
        for cat in tqdm(cats):
            url_lists[cat] = make_urllist(cat, today)
            count += len(url_lists[cat])
        logger.info(f"found new url : {count}")
        send_info(cats, url_lists, today)

    else:
        for cat in tqdm(cats):
            url_lists[cat] = make_urllist(cat, today)
            count += len(url_lists[cat])
        logger.info(f"last found new url : {count}")
        send_info(cats, url_lists, today)

        today = now
        last_url = {100:"",101:"",102:"",103:"",104:"",105:""}
        create_csv(cats, today)


    time.sleep(10)





