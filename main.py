from kafka import KafkaProducer
import time
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from datetime import datetime
import time
import os
from tqdm import tqdm
import re
import random
import concurrent.futures
from hdfs import InsecureClient
import logging


producer = KafkaProducer(acks=0,
                         compression_type='gzip',
                         bootstrap_servers=['kafka:9092'],
                         value_serializer=lambda x:json.dumps(x).encode('utf-8'),
                          api_version=(2,)
                         )

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

##data 폴더 생성
try:
    if not os.path.exists("data"):
        os.mkdir("data")
except OSError:
    logger.info('Error: Creating directory. ' +  "data")


def cleanText(readData):
  return re.sub(" {2,}", ' ', re.sub("[^\w\s\t\n\v\f\r]", '', readData))


def generate_weighted_random_number(min, max):
    # 0.5에서 1.5 사이에서 소수점 둘째 자리까지 랜덤 숫자 생성
    random_number = random.uniform(min, max)

    # 0.5가 더 자주 나오게 하기 위해 조절된 확률 사용
    if random.random() < 0.7:  # 예시로 0.5가 더 자주 나오도록 조절 (0.7은 실험에 따라 조절 가능)
        return 0.4
    else:
        if random.random() < 0.999:
            return random_number
        else:
            return 3

def make_urllist(cat, date, repeat):
    global last_url
    page_num = 0
    first_url = ""
    urllist= []
    first_flag = 0
    last_flag = 0

    for i in range(repeat):
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

        time.sleep(generate_weighted_random_number(0.8, 1.2))

    return urllist


def send_info(cats, url_lists, today, words_to_search):
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
                createDate = soup.find(class_="media_end_head_info_datestamp_time _ARTICLE_DATE_TIME")['data-date-time']
                #createDate_val = convert_date(createDate.text)
                createDate_val = createDate
            except:
                continue
                #createDate_val = None
                #print(url)

            ##수정일자
            #media_end_head_info_datestamp_time _ARTICLE_MODIFY_DATE_TIME
            # try:
            #     modifyDate = soup.find(class_="media_end_head_info_datestamp_time _ARTICLE_MODIFY_DATE_TIME")
            #     modifyDate_val = modifyDate.text
            # except:
            #     modifyDate_val = None


            ##본문내용
            #dic_area 안 br카테고리 txt
            try:
                #text_list = ""

                brs = soup.find(id="dic_area")
                #print(len(brs))
                #brs = lis.findAll("br")
                #brs.findAll("br")

                text_list = cleanText(brs.get_text(strip=True))

                themes_data = count_words_parallel(text_list, words_to_search)
                #for br in brs:
                #    text_list.append(br.get_text(strip=True))
                #print(text_list)
            except:
                print(url)


            ##이미지 URL
            #end_photo_org
            # try:
            #     img_url_list = []

            #     urs = soup.findAll(class_="end_photo_org")
            #     #print(len(urs))
            #     for ur in urs:
            #         img_url_list.append(ur.img['data-src'])
            #     #print(img_url_list)
            # except:
            #     print(url)

            ##작성기자 기자 이메일
            #byline_s
            # try:
            #     author_list = []

            #     authors = soup.findAll(class_="byline_s")
            #     #print(len(authors))
            #     for au in authors:
            #         author_list.append(au.text)
            #     #print(author_list)
            # except:
            #     print(url)

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
                    'create_date': createDate_val,
                    'content': text_list,
                    'themes': themes_data,
                }
            producer.send('news_crawling', value=news_data)



            last_url[cat] = url

            time.sleep(generate_weighted_random_number(0.8, 1.2))



def get_thema():
    # HDFS 클라이언트를 생성합니다.
    hdfs_client = InsecureClient('http://namenode:9870', user='root')

    # HDFS 경로에서 JSON 파일을 읽어옵니다.
    hdfs_path = '/thema/themaju.json'

    with hdfs_client.read(hdfs_path) as hdfs_file:
        # JSON 데이터를 읽어옵니다.
        json_data = hdfs_file.read()

    # JSON 데이터를 파싱합니다.
    data = json.loads(json_data.decode('utf-8'))

    return data

def count_word(text, word):
    return word, text.count(word)

def count_words_parallel(text, words_to_count):
    word_counts = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_word = {executor.submit(count_word, text, word): word for word in words_to_count}
        for future in concurrent.futures.as_completed(future_to_word):
            word, count = future.result()
            if count > 0:
                word_counts[word] = count

    return word_counts


if __name__ == "__main__":
    logger.info("코드가 시작됩니다.")
    
    ##오늘날짜얻기
    #today = datetime.today().strftime('%Y%m%d')
    #print(today)

    cats = [100, 101, 102, 103 ,104, 105]
    last_url = {100:"",101:"",102:"",103:"",104:"",105:""}

    #원본
    # tema = get_thema()
    # words_to_search = list(tema.keys())

    tema = get_thema()
    tema = list(tema.values())

    # 빈 리스트 생성
    words_to_search = []

    # 모든 하위 리스트를 하나의 리스트로 병합
    for sublist in tema:
        words_to_search.extend(sublist)




    crawling_first = True
    repeat = 0
    while True:

        today = datetime.today().strftime('%Y%m%d')

        if crawling_first == True:
            repeat = 1
        else:
            repeat = 10000


        url_lists = {}
        count = 0

        for cat in tqdm(cats):
            url_lists[cat] = make_urllist(cat, today, repeat)
            count += len(url_lists[cat])
        logger.info(f"found new url : {count}")
        send_info(cats, url_lists, today, words_to_search)


        crawling_first = False
        time.sleep(5)

