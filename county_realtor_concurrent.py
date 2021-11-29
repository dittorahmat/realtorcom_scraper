# import libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
from requests_html import HTMLSession
import concurrent.futures
from dateutil.relativedelta import *
import sqlalchemy

# state_code='CT'
# search_location = 'Bristol County, RI'
# search_location = 'Kent County, RI'
search_location = [
'Litchfield County, CT',
'Middlesex County, CT',
]

#Scrape from these locations, it can be county (Litchfield County, CT), city (Tolland, CT) or zip code
df = pd.read_csv('search_location.csv')

s = requests.Session()
def county_sale(search_location):
    list_prop = []

    start_time = time.time()

    print(f'Start scraping for sale in {search_location}\n')
    offset = 0
    while True:
        #print(f'Scraping about {offset} data from {search_location}\n')
        headers = {
            'authority': 'www.realtor.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            'tracestate': '1022681@nr=0-1-378584-129741352-34cbc3d69db9f4cc----1637321974075',
            'traceparent': '00-5cdb95347943fe9c81e1957b38bf17e0-34cbc3d69db9f4cc-01',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
            'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM3ODU4NCIsImFwIjoiMTI5NzQxMzUyIiwiaWQiOiIzNGNiYzNkNjlkYjlmNGNjIiwidHIiOiI1Y2RiOTUzNDc5NDNmZTljODFlMTk1N2IzOGJmMTdlMCIsInRpIjoxNjM3MzIxOTc0MDc1LCJ0ayI6IjEwMjI2ODEifX0=',
            'content-type': 'application/json',
            'accept': 'application/json',
            'sec-ch-ua-platform': '"Windows"',
            'origin': 'https://www.realtor.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.realtor.com/realestateandhomes-search/New-York/pg-6',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': '__vst=df3bfa89-166e-4fff-909b-3f228014f031; _rdt_uuid=1633298254141.32500c6e-c251-4471-b0bb-81e9ce534bd7; ajs_anonymous_id=%22e0d4157d-debd-4296-8d2a-2cb00ec31bcb%22; s_ecid=MCMID%7C42255103375102245280269219361012088179; _gcl_au=1.1.1235274536.1633298259; _ga=GA1.2.1073586179.1633298259; _pin_unauth=dWlkPU1ERTBZVEU0TWpjdFlqSmlNaTAwWVRVekxUazVPRGN0TVdGa09XUTBZamMyWTJGaQ; split=n; split_tcv=161; permutive-id=9250a0c1-2e7c-476a-ba41-1fcf73622a5e; ab.storage.userId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22visitor_df3bfa89-166e-4fff-909b-3f228014f031%22%2C%22c%22%3A1634766288337%2C%22l%22%3A1634766288337%7D; ab.storage.deviceId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22b6e060b7-2421-16ab-5464-e38ddd0a2b5a%22%2C%22c%22%3A1634766288349%2C%22l%22%3A1634766288349%7D; _pxvid=f2fb800d-31ee-11ec-9513-474c47634c6b; G_ENABLED_IDPS=google; __ssnstarttime=1637034754; AMCVS_8853394255142B6A0A4C98A4%40AdobeOrg=1; __split=71; _tac=false~self|not-available; _ta=us~1~89df68bc35032d48c293adce8574aa42; srchID=81f625257cb4467487ea87626789941e; user_activity=return; pxcts=09e07531-4691-11ec-b910-f50bc588fbe6; AMCVS_AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=1; __qca=P0-667222422-1637034965976; _gid=GA1.2.1649618996.1637138558; threshold_value=52; automation=false; clstr=; clstr_tcv=; bcc=false; bcvariation=SRPBCRR%3Av1%3Adesktop; last_viewed_property=%2Frealestateandhomes-detail%2F227-S-Thames-St_Norwich_CT_06360_M39824-83717; userStatus=return_user; previousUrl=%2Frealestateandhomes-detail%2F227-S-Thames-St_Norwich_CT_06360_M39824-83717; g_state={"i_p":1637830129448,"i_l":3}; QSI_SI_2a9qZZvRnb5FaZf_intercept=true; __ssn=7c1c4f29-c535-4a52-927f-dde8e3fda6b7; __edwssnstarttime=1637232336; _rdc-next_session=TkJBYkN1TzVzV3lkd083SVBUYm1rV3VUMUdvR0psN2s1TjNjRlQ1ZXhQWHdzczVJOG0yNUprZHNTQmYyOWhqMVl3QnZETHlKTGRzQnBNYWdtb1pHekNhNG9Ec2NwVmpqc0MrZVdiTSs2cFZKT0dZTkNsY2QySWRHRWZaR2lWUlcySDZkUVEzNVRoOXQzdm1PRDRTWDRDTFVVeklDOGhMRjdwNVFXRTVRR0svOHBESUU5eTRJZXFJaE5paTU2elBGeVZDSEM4ckxpTG5iWFk3Y0dLT3RvZ3hyaDQ5R1lhdUpObStodExFU2tPQWpuRGxNT3d5cVo0OE5vNkt3NnFmVmxzQmY4MzV2dFNlNDJCQ2E2YlZpdTFucXFjbmk3MitMMHRJOHM4L0s5VHNJYnVCSEozaFZCRloyUGpwTEQ0c1ZXY2pYTityUDY0a05TK2Vsc1pDb0FKaU93NXdCV29FSGlQQmpZWERDY3M2dEFHRVlUMi9sTW82L3JCbUZtN1BlVzkxQWo2cytLdTdBVnhkZXJPV1pMWWczK1dlRDFtN0JweVk1bmdaNFRLcWRkUktoYlVEQzRqM3grclhhK1V2bVUrRUlKdG1PcWdadkRjcWwxSUY3UFE9PS0tSnFpdW1CSTUzbE90RWtCZXRkL2p3dz09--3f5851015ba09ed32a6a49077d648e275f2ef079; AWSALBTG=ZoK/Z0NweS72BXJHQN91PL9T5YT3EbZMonYyrIikC9xDMzslllbB6XsqZ0l7VAZR3z2ccQyIrwtlenIj4wC1NXUd1taSOhn8G3rAGyOp8ssLlMImBaqBK+N3MQG1kkxw2PTTtbR6Tszc4xXrBPJXaUjuwNzVoEUL1peDTbfHJBiA; AWSALBTGCORS=ZoK/Z0NweS72BXJHQN91PL9T5YT3EbZMonYyrIikC9xDMzslllbB6XsqZ0l7VAZR3z2ccQyIrwtlenIj4wC1NXUd1taSOhn8G3rAGyOp8ssLlMImBaqBK+N3MQG1kkxw2PTTtbR6Tszc4xXrBPJXaUjuwNzVoEUL1peDTbfHJBiA; AWSALB=vgJXQaqND+SlLRmhcoSnCkdNxmBcdTnM/SUl34ZUGxhHbEf+3Kpq2PlWf1W0+U1DOFgymWixn+sdREQZnbIbO5Y1dBS0Va9yghWzNaXhqs7Ke00BJjcFxD0XwNX0; AWSALBCORS=vgJXQaqND+SlLRmhcoSnCkdNxmBcdTnM/SUl34ZUGxhHbEf+3Kpq2PlWf1W0+U1DOFgymWixn+sdREQZnbIbO5Y1dBS0Va9yghWzNaXhqs7Ke00BJjcFxD0XwNX0; _derived_epik=dj0yJnU9ZFN4SHlILUh6TlU1YmdQc0RTRElGVE1YRW9nU2h4QlAmbj1OTmRMcElGZVNReG1Dc3haQWVfb2xnJm09MSZ0PUFBQUFBR0dXTWFBJnJtPTEmcnQ9QUFBQUFHR1dNYUE; QSI_HistorySession=https%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F1401-Avenue-Y_Brooklyn_NY_11235_M90632-93776~1637225315017%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F1163-E-99th-St_Brooklyn_NY_11236_M31421-56003~1637225391146%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F227-S-Thames-St_Norwich_CT_06360_M39824-83717~1637232337889%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F1163-E-99th-St_Brooklyn_NY_11236_M31421-56003~1637232380314%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateagents%2FBin-Hu-Zhan_Brooklyn_NY_2821789~1637233031710%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F3-Hamlin-Ave_West-Babylon_NY_11704_M31141-06793~1637235557368%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F35-Cedar-Rd_Rocky-Point_NY_11778_M46361-81540~1637236134683%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F115-Pine-Hills-Dr_Pine-City_NY_14871_M33205-51677~1637239368925%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F164-S-Main-St_Black-River_NY_13612_M38127-88402~1637273713345%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F687-Main-St_Cairo_NY_12413_M44954-50115~1637273813908%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F164-S-Main-St_Black-River_NY_13612_M38127-88402~1637276972647%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F687-Main-St_Cairo_NY_12413_M44954-50115~1637277009828%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York%2Fpg-1~1637278167036%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York%2Fpg-2~1637303091093%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York~1637305459365%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York%2Fpg-2~1637306656588; srchID=a544a0b2f2e145fc8f9e33105827fea1; permutive-session=%7B%22session_id%22%3A%225154ba4c-2cee-4c32-bb02-5fc7f4e03648%22%2C%22last_updated%22%3A%222021-11-19T11%3A38%3A35.273Z%22%7D; criteria=pg%3D6%26sprefix%3D%252Frealestateandhomes-search%26area_type%3Dstate%26search_type%3Dstate%26state%3DNew%2520York%26state_code%3DNY%26state_id%3DNY%26lat%3D42.9212421566579%26long%3D-75.5965453188092%26loc%3DNew%2520York%26locSlug%3DNew-York; _pxff_bdd=2000; _pxff_cde=5,10; AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=-1124106680%7CMCIDTS%7C18950%7CMCMID%7C42255103375102245280269219361012088179%7CMCAAMLH-1637926733%7C3%7CMCAAMB-1637926733%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1637329133s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.2.0; _tas=27rjaxndc4r; ab.storage.sessionId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%221418d025-6684-de54-33d0-2e82cd77bd30%22%2C%22e%22%3A1637323738230%2C%22c%22%3A1637321919950%2C%22l%22%3A1637321938230%7D; _gat=1; _uetsid=51b92640478211ecb152af1384445b7b; _uetvid=ed184f50249411ecba7957ba0c2ce1dc; AMCV_AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=-1124106680%7CMCIDTS%7C18950%7CMCMID%7C37077437028931217065766293041811363460%7CMCOPTOUT-1637329155s%7CNONE%7CvVersion%7C5.2.0; adcloud={%22_les_v%22:%22y%2Crealtor.com%2C1637323755%22}; _px3=0ceb5ce418926a6d6caa5609b03388b2ab49537b39413e9b2efec9d3a7d86d95:FYiOr6ubnGIU+YTp+57XdvKKjLQ4zNM6dawg61ca+sG8Z+Kz4qB5V5Sa1gdSUeEFemE2ljUZ6MA95Pzhny4ZbQ==:1000:CLTKjotRNsROIpfs5tREPVspQo/m6ID4ZS9m82HnLMISgyer52OfrCxPmAaduplGKFcn0Zk2CY3uVVmH3jOB8Se6jfONZmjaRMEuT5d9bZ7KRJ5iZoe1LAlrGiAAmw7hsCAxv5DSsoL+ZnCrA/LQXAngz2PjtMQvozvEv+k5+U+LKRiBwUg9aYyep5tqLYkObfejV5HPR0Gpz0NyRiX/9g==; last_ran=-1; last_ran_threshold=1637321974065',
        }

        headers1 = {
            'authority': 'www.realtor.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
            'content-type': 'application/json',
            'accept': 'application/json',
            'sec-ch-ua-platform': '"Windows"',
            'origin': 'https://www.realtor.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.realtor.com/realestateandhomes-search/New-York/pg-6',
            'accept-language': 'en-US,en;q=0.9',
        }


        params = (
            ('client_id', 'rdc-x'),
            ('schema', 'vesta'),
        )

        data1 = ''
        # CT, NY, MA, RI, NJ, PA, VT, ME, NH

        # data2 = '"variables":{"query":{"status":["for_sale","ready_to_build"],"primary":true,'
        data2 = '"variables":{"query":{"status":["for_sale","ready_to_build"],"primary":true,"search_location":{"location":'
        # data3 = f'"state_code":"{state_code}"'
        data3 = f'"{search_location}"'
        # data4 = '},"client_data":{"device_data":{"device_type":"web"},"user_data":{"last_view_timestamp":-1}},"limit":200,'
        data4 = '}},"client_data":{"device_data":{"device_type":"web"},"user_data":{"last_view_timestamp":-1}},"limit":200,'
        data5 = f'"offset":{offset},'
        data6 = '"zohoQuery":{"silo":"search_result_page","location":"New York","property_status":"for_sale","filters":{},"page_index":"7"},"sort_type":"relevant","geoSupportedSlug":"","by_prop_type":["home"]},"callfrom":"SRP","nrQueryType":"MAIN_SRP","visitor_id":"df3bfa89-166e-4fff-909b-3f228014f031","isClient":true,"seoPayload":{"asPath":"/realestateandhomes-search/New-York/pg-7","pageType":{"silo":"search_result_page","status":"for_sale"},"county_needed_for_uniq":false}}'

        data = data1 + data2 + data3 + data4 + data5 + data6
        proxies = {
            

        }
        try:
            response = s.post('https://www.realtor.com/api/v1/hulk', headers=headers, params=params, data=data,
                              #proxies=proxies
                              )

            # NB. Original query string below. It seems impossible to parse and
            # reproduce query strings 100% accurately so the one below is given
            # in case the reproduced version is not "correct".
            # response = requests.post('https://www.realtor.com/api/v1/hulk?client_id=rdc-x&schema=vesta', headers=headers, data=data)
            response_json = response.json()

            data = response_json['data']['home_search']['results']
            max_list_per_page = len(data)
        except Exception as e:
            print('Maximum amount of search page reached')
            #print(e)
            break

        s1 = HTMLSession()
        # full loop for 1 page of search property data
        # 200 is max limit for search listings per page
        #with concurrent.futures.ThreadPoolExecutor() as executor:
        #    executor.map(property_scraper,data)        
        for i in range(0, max_list_per_page):
            dict_prop = property_scraper(data[i])
            list_prop.append(dict_prop)
            #print(i)
        
        # iterate
        offset += 200
        if len(data) < 200:
            break

    how_long = time.time() - start_time

    # df_prop=pd.DataFrame(list_prop)
    # df_prop.to_excel(f'realtor-{state_code}-all-properties.xls')
    df_prop = pd.DataFrame(list_prop)
    df_prop.to_csv(f'export/{search_location}-sale.csv', index=False)
    rows = len(df_prop)
    insert_database(df_prop)
    print(f'Finished scraping {rows} for sale properties from {search_location} in {how_long} seconds \n')

def county_sold(search_location):
    list_prop = []

    start_time = time.time()

    print(f'Start scraping recently sold in {search_location}\n')
    offset = 0
    while True:
        #print(f'Scraping about {offset} data from {search_location}\n')
        headers = {
            'authority': 'www.realtor.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            'tracestate': '1022681@nr=0-1-378584-129741352-34cbc3d69db9f4cc----1637321974075',
            'traceparent': '00-5cdb95347943fe9c81e1957b38bf17e0-34cbc3d69db9f4cc-01',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
            'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM3ODU4NCIsImFwIjoiMTI5NzQxMzUyIiwiaWQiOiIzNGNiYzNkNjlkYjlmNGNjIiwidHIiOiI1Y2RiOTUzNDc5NDNmZTljODFlMTk1N2IzOGJmMTdlMCIsInRpIjoxNjM3MzIxOTc0MDc1LCJ0ayI6IjEwMjI2ODEifX0=',
            'content-type': 'application/json',
            'accept': 'application/json',
            'sec-ch-ua-platform': '"Windows"',
            'origin': 'https://www.realtor.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.realtor.com/realestateandhomes-search/New-York/pg-6',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': '__vst=df3bfa89-166e-4fff-909b-3f228014f031; _rdt_uuid=1633298254141.32500c6e-c251-4471-b0bb-81e9ce534bd7; ajs_anonymous_id=%22e0d4157d-debd-4296-8d2a-2cb00ec31bcb%22; s_ecid=MCMID%7C42255103375102245280269219361012088179; _gcl_au=1.1.1235274536.1633298259; _ga=GA1.2.1073586179.1633298259; _pin_unauth=dWlkPU1ERTBZVEU0TWpjdFlqSmlNaTAwWVRVekxUazVPRGN0TVdGa09XUTBZamMyWTJGaQ; split=n; split_tcv=161; permutive-id=9250a0c1-2e7c-476a-ba41-1fcf73622a5e; ab.storage.userId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22visitor_df3bfa89-166e-4fff-909b-3f228014f031%22%2C%22c%22%3A1634766288337%2C%22l%22%3A1634766288337%7D; ab.storage.deviceId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22b6e060b7-2421-16ab-5464-e38ddd0a2b5a%22%2C%22c%22%3A1634766288349%2C%22l%22%3A1634766288349%7D; _pxvid=f2fb800d-31ee-11ec-9513-474c47634c6b; G_ENABLED_IDPS=google; __ssnstarttime=1637034754; AMCVS_8853394255142B6A0A4C98A4%40AdobeOrg=1; __split=71; _tac=false~self|not-available; _ta=us~1~89df68bc35032d48c293adce8574aa42; srchID=81f625257cb4467487ea87626789941e; user_activity=return; pxcts=09e07531-4691-11ec-b910-f50bc588fbe6; AMCVS_AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=1; __qca=P0-667222422-1637034965976; _gid=GA1.2.1649618996.1637138558; threshold_value=52; automation=false; clstr=; clstr_tcv=; bcc=false; bcvariation=SRPBCRR%3Av1%3Adesktop; last_viewed_property=%2Frealestateandhomes-detail%2F227-S-Thames-St_Norwich_CT_06360_M39824-83717; userStatus=return_user; previousUrl=%2Frealestateandhomes-detail%2F227-S-Thames-St_Norwich_CT_06360_M39824-83717; g_state={"i_p":1637830129448,"i_l":3}; QSI_SI_2a9qZZvRnb5FaZf_intercept=true; __ssn=7c1c4f29-c535-4a52-927f-dde8e3fda6b7; __edwssnstarttime=1637232336; _rdc-next_session=TkJBYkN1TzVzV3lkd083SVBUYm1rV3VUMUdvR0psN2s1TjNjRlQ1ZXhQWHdzczVJOG0yNUprZHNTQmYyOWhqMVl3QnZETHlKTGRzQnBNYWdtb1pHekNhNG9Ec2NwVmpqc0MrZVdiTSs2cFZKT0dZTkNsY2QySWRHRWZaR2lWUlcySDZkUVEzNVRoOXQzdm1PRDRTWDRDTFVVeklDOGhMRjdwNVFXRTVRR0svOHBESUU5eTRJZXFJaE5paTU2elBGeVZDSEM4ckxpTG5iWFk3Y0dLT3RvZ3hyaDQ5R1lhdUpObStodExFU2tPQWpuRGxNT3d5cVo0OE5vNkt3NnFmVmxzQmY4MzV2dFNlNDJCQ2E2YlZpdTFucXFjbmk3MitMMHRJOHM4L0s5VHNJYnVCSEozaFZCRloyUGpwTEQ0c1ZXY2pYTityUDY0a05TK2Vsc1pDb0FKaU93NXdCV29FSGlQQmpZWERDY3M2dEFHRVlUMi9sTW82L3JCbUZtN1BlVzkxQWo2cytLdTdBVnhkZXJPV1pMWWczK1dlRDFtN0JweVk1bmdaNFRLcWRkUktoYlVEQzRqM3grclhhK1V2bVUrRUlKdG1PcWdadkRjcWwxSUY3UFE9PS0tSnFpdW1CSTUzbE90RWtCZXRkL2p3dz09--3f5851015ba09ed32a6a49077d648e275f2ef079; AWSALBTG=ZoK/Z0NweS72BXJHQN91PL9T5YT3EbZMonYyrIikC9xDMzslllbB6XsqZ0l7VAZR3z2ccQyIrwtlenIj4wC1NXUd1taSOhn8G3rAGyOp8ssLlMImBaqBK+N3MQG1kkxw2PTTtbR6Tszc4xXrBPJXaUjuwNzVoEUL1peDTbfHJBiA; AWSALBTGCORS=ZoK/Z0NweS72BXJHQN91PL9T5YT3EbZMonYyrIikC9xDMzslllbB6XsqZ0l7VAZR3z2ccQyIrwtlenIj4wC1NXUd1taSOhn8G3rAGyOp8ssLlMImBaqBK+N3MQG1kkxw2PTTtbR6Tszc4xXrBPJXaUjuwNzVoEUL1peDTbfHJBiA; AWSALB=vgJXQaqND+SlLRmhcoSnCkdNxmBcdTnM/SUl34ZUGxhHbEf+3Kpq2PlWf1W0+U1DOFgymWixn+sdREQZnbIbO5Y1dBS0Va9yghWzNaXhqs7Ke00BJjcFxD0XwNX0; AWSALBCORS=vgJXQaqND+SlLRmhcoSnCkdNxmBcdTnM/SUl34ZUGxhHbEf+3Kpq2PlWf1W0+U1DOFgymWixn+sdREQZnbIbO5Y1dBS0Va9yghWzNaXhqs7Ke00BJjcFxD0XwNX0; _derived_epik=dj0yJnU9ZFN4SHlILUh6TlU1YmdQc0RTRElGVE1YRW9nU2h4QlAmbj1OTmRMcElGZVNReG1Dc3haQWVfb2xnJm09MSZ0PUFBQUFBR0dXTWFBJnJtPTEmcnQ9QUFBQUFHR1dNYUE; QSI_HistorySession=https%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F1401-Avenue-Y_Brooklyn_NY_11235_M90632-93776~1637225315017%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F1163-E-99th-St_Brooklyn_NY_11236_M31421-56003~1637225391146%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F227-S-Thames-St_Norwich_CT_06360_M39824-83717~1637232337889%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F1163-E-99th-St_Brooklyn_NY_11236_M31421-56003~1637232380314%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateagents%2FBin-Hu-Zhan_Brooklyn_NY_2821789~1637233031710%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F3-Hamlin-Ave_West-Babylon_NY_11704_M31141-06793~1637235557368%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F35-Cedar-Rd_Rocky-Point_NY_11778_M46361-81540~1637236134683%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F115-Pine-Hills-Dr_Pine-City_NY_14871_M33205-51677~1637239368925%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F164-S-Main-St_Black-River_NY_13612_M38127-88402~1637273713345%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F687-Main-St_Cairo_NY_12413_M44954-50115~1637273813908%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F164-S-Main-St_Black-River_NY_13612_M38127-88402~1637276972647%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-detail%2F687-Main-St_Cairo_NY_12413_M44954-50115~1637277009828%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York%2Fpg-1~1637278167036%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York%2Fpg-2~1637303091093%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York~1637305459365%7Chttps%3A%2F%2Fwww.realtor.com%2Frealestateandhomes-search%2FNew-York%2Fpg-2~1637306656588; srchID=a544a0b2f2e145fc8f9e33105827fea1; permutive-session=%7B%22session_id%22%3A%225154ba4c-2cee-4c32-bb02-5fc7f4e03648%22%2C%22last_updated%22%3A%222021-11-19T11%3A38%3A35.273Z%22%7D; criteria=pg%3D6%26sprefix%3D%252Frealestateandhomes-search%26area_type%3Dstate%26search_type%3Dstate%26state%3DNew%2520York%26state_code%3DNY%26state_id%3DNY%26lat%3D42.9212421566579%26long%3D-75.5965453188092%26loc%3DNew%2520York%26locSlug%3DNew-York; _pxff_bdd=2000; _pxff_cde=5,10; AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=-1124106680%7CMCIDTS%7C18950%7CMCMID%7C42255103375102245280269219361012088179%7CMCAAMLH-1637926733%7C3%7CMCAAMB-1637926733%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1637329133s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.2.0; _tas=27rjaxndc4r; ab.storage.sessionId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%221418d025-6684-de54-33d0-2e82cd77bd30%22%2C%22e%22%3A1637323738230%2C%22c%22%3A1637321919950%2C%22l%22%3A1637321938230%7D; _gat=1; _uetsid=51b92640478211ecb152af1384445b7b; _uetvid=ed184f50249411ecba7957ba0c2ce1dc; AMCV_AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=-1124106680%7CMCIDTS%7C18950%7CMCMID%7C37077437028931217065766293041811363460%7CMCOPTOUT-1637329155s%7CNONE%7CvVersion%7C5.2.0; adcloud={%22_les_v%22:%22y%2Crealtor.com%2C1637323755%22}; _px3=0ceb5ce418926a6d6caa5609b03388b2ab49537b39413e9b2efec9d3a7d86d95:FYiOr6ubnGIU+YTp+57XdvKKjLQ4zNM6dawg61ca+sG8Z+Kz4qB5V5Sa1gdSUeEFemE2ljUZ6MA95Pzhny4ZbQ==:1000:CLTKjotRNsROIpfs5tREPVspQo/m6ID4ZS9m82HnLMISgyer52OfrCxPmAaduplGKFcn0Zk2CY3uVVmH3jOB8Se6jfONZmjaRMEuT5d9bZ7KRJ5iZoe1LAlrGiAAmw7hsCAxv5DSsoL+ZnCrA/LQXAngz2PjtMQvozvEv+k5+U+LKRiBwUg9aYyep5tqLYkObfejV5HPR0Gpz0NyRiX/9g==; last_ran=-1; last_ran_threshold=1637321974065',
        }

        headers1 = {
            'authority': 'www.realtor.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
            'content-type': 'application/json',
            'accept': 'application/json',
            'sec-ch-ua-platform': '"Windows"',
            'origin': 'https://www.realtor.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.realtor.com/realestateandhomes-search/New-York/pg-6',
            'accept-language': 'en-US,en;q=0.9',
        }


        params = (
            ('client_id', 'rdc-x'),
            ('schema', 'vesta'),
        )
        #sold listings can only be get up to 6 months prior
        now=datetime.datetime.now() - relativedelta(months=6)
        now=now.strftime('%Y-%m-%d')
        
        data1 = '{"query":"\\n\\nfragment geoStatisticsFields on Geo {\\n  geo_statistics(group_by: property_type) {\\n    housing_market {\\n      by_prop_type(type :[\\"home\\"]){\\n        type\\n        attributes {\\n          median_listing_price\\n        }\\n      }\\n      median_listing_price\\n    }\\n  }\\n}\\n\\nquery ConsumerSearchQuery($query: HomeSearchCriteria!, $limit: Int, $offset: Int, $sort: [SearchAPISort], $sort_type: SearchSortType, $client_data: JSON, $geoSupportedSlug: String!, $bucket: SearchAPIBucket, $by_prop_type: [String])\\n{\\n  home_search: home_search(query: $query,\\n    sort: $sort,\\n    limit: $limit,\\n    offset: $offset,\\n    sort_type: $sort_type,\\n    client_data: $client_data,\\n    bucket: $bucket,\\n  ){\\n    count\\n    total\\n    results {\\n      property_id\\n      list_price\\n      primary_photo (https: true){\\n        href\\n      }\\n      source {\\n        id\\n        agents{\\n          office_name\\n        }\\n        type\\n        spec_id\\n        plan_id\\n      }\\n      community {\\n        property_id\\n        description {\\n          name\\n        }\\n        advertisers{\\n          office{\\n            hours\\n            phones {\\n              type\\n              number\\n            }\\n          }\\n          builder {\\n            fulfillment_id\\n          }\\n        }\\n      }\\n      products {\\n        brand_name\\n        products\\n      }\\n      listing_id\\n      matterport\\n      virtual_tours{\\n        href\\n        type\\n      }\\n      status\\n      permalink\\n      price_reduced_amount\\n      other_listings{rdc {\\n      listing_id\\n      status\\n      listing_key\\n      primary\\n    }}\\n      description{\\n        beds\\n        baths\\n        baths_full\\n        baths_half\\n        baths_1qtr\\n        baths_3qtr\\n        garage\\n        stories\\n        type\\n        sub_type\\n        lot_sqft\\n        sqft\\n        year_built\\n        sold_price\\n        sold_date\\n        name\\n      }\\n      location{\\n        street_view_url\\n        address{\\n          line\\n          postal_code\\n          state\\n          state_code\\n          city\\n          coordinate {\\n            lat\\n            lon\\n          }\\n        }\\n        county {\\n          name\\n          fips_code\\n        }\\n      }\\n      tax_record {\\n        public_record_id\\n      }\\n      lead_attributes {\\n        show_contact_an_agent\\n        opcity_lead_attributes {\\n          cashback_enabled\\n          flip_the_market_enabled\\n        }\\n        lead_type\\n      }\\n      open_houses {\\n        start_date\\n        end_date\\n        description\\n        methods\\n        time_zone\\n        dst\\n      }\\n      flags{\\n        is_coming_soon\\n        is_pending\\n        is_foreclosure\\n        is_contingent\\n        is_new_construction\\n        is_new_listing (days: 14)\\n        is_price_reduced (days: 30)\\n        is_plan\\n        is_subdivision\\n      }\\n      list_date\\n      last_update_date\\n      coming_soon_date\\n      photos(limit: 2, https: true){\\n        href\\n      }\\n      tags\\n      branding {\\n        type\\n        photo\\n        name\\n      }\\n    }\\n  }\\n  geo(slug_id: $geoSupportedSlug) {\\n    parents {\\n      geo_type\\n      slug_id\\n      name\\n    }\\n    geo_statistics(group_by: property_type) {\\n      housing_market {\\n        by_prop_type(type: $by_prop_type){\\n          type\\n           attributes{\\n            median_listing_price\\n            median_lot_size\\n            median_sold_price\\n            median_price_per_sqft\\n            median_days_on_market\\n          }\\n        }\\n        listing_count\\n        median_listing_price\\n        median_rent_price\\n        median_price_per_sqft\\n        median_days_on_market\\n        median_sold_price\\n        month_to_month {\\n          active_listing_count_percent_change\\n          median_days_on_market_percent_change\\n          median_listing_price_percent_change\\n          median_listing_price_sqft_percent_change\\n        }\\n      }\\n    }\\n    recommended_cities: recommended(query: {geo_search_type: city, limit: 20}) {\\n      geos {\\n        ... on City {\\n          city\\n          state_code\\n          geo_type\\n          slug_id\\n        }\\n        ...geoStatisticsFields\\n      }\\n    }\\n    recommended_neighborhoods: recommended(query: {geo_search_type: neighborhood, limit: 20}) {\\n      geos {\\n        ... on Neighborhood {\\n          neighborhood\\n          city\\n          state_code\\n          geo_type\\n          slug_id\\n        }\\n        ...geoStatisticsFields\\n      }\\n    }\\n    recommended_counties: recommended(query: {geo_search_type: county, limit: 20}) {\\n      geos {\\n        ... on HomeCounty {\\n          county\\n          state_code\\n          geo_type\\n          slug_id\\n        }\\n        ...geoStatisticsFields\\n      }\\n    }\\n    recommended_zips: recommended(query: {geo_search_type: postal_code, limit: 20}) {\\n      geos {\\n        ... on PostalCode {\\n          postal_code\\n          geo_type\\n          slug_id\\n        }\\n        ...geoStatisticsFields\\n      }\\n    }\\n  }\\n}",'
        data2 = '"variables":{"query":{"status":["sold"],"sold_date":{"min":'
        data3 = f'"{now}T10:59:22.812Z"'
        data4 = '},"search_location":{"location":'
        data5 = f'"{search_location}"'
        data6 = '}},"client_data":{"device_data":{"device_type":"web"},"user_data":{"last_view_timestamp":1638097162759}},"limit":200,'
        data7 = f'"offset":{offset},'
        data8 = '"zohoQuery":{"silo":"search_result_page","location":"Windham County, CT","property_status":"for_sale","filters":{"show_listings":["rs"],"pending":true,"contingent":true,"new_construction":null,"foreclosure":true}},"geoSupportedSlug":"Windham-County_CT","resetMap":"2021-11-26T23:19:17.503Z0.6503419396373429","sort":[{"field":"sold_date","direction":"desc"},{"field":"photo_count","direction":"desc"}],"by_prop_type":["home"]},"callfrom":"SRP","nrQueryType":"MAIN_SRP","visitor_id":"df3bfa89-166e-4fff-909b-3f228014f031","isClient":true,"seoPayload":{"asPath":"/realestateandhomes-search/Windham-County_CT/show-recently-sold","pageType":{"silo":"search_result_page","status":"for_sale"},"county_needed_for_uniq":false}}'


        data=data1+data2+data3+data4+data5+data6+data7+data8
        proxies = {
            "http": "http://88.99.35.82:3128/",
            "https": "https://88.99.35.82:3128/",
            #"http": "http://bzgamebu-rotate:gbb7h9r3fzkr@p.webshare.io:80/",
            #"https": "http://bzgamebu-rotate:gbb7h9r3fzkr@p.webshare.io:80/",
            

        }
        try:
            response = s.post('https://www.realtor.com/api/v1/hulk', headers=headers, params=params, data=data,
                              #proxies=proxies
                              )

            # NB. Original query string below. It seems impossible to parse and
            # reproduce query strings 100% accurately so the one below is given
            # in case the reproduced version is not "correct".
            # response = requests.post('https://www.realtor.com/api/v1/hulk?client_id=rdc-x&schema=vesta', headers=headers, data=data)
            response_json = response.json()

            data = response_json['data']['home_search']['results']
            max_list_per_page = len(data)
        except Exception as e:
            print('Maximum amount of search page reached')
            #print(e)
            break

        s1 = HTMLSession()
        # full loop for 1 page of search property data
        # 200 is max limit for search listings per page
        #with concurrent.futures.ThreadPoolExecutor() as executor:
        #    executor.map(property_scraper,data)        
        for i in range(0, max_list_per_page):
            dict_prop = property_scraper(data[i])
            list_prop.append(dict_prop)
            #print(i)
        
        # iterate
        offset += 200
        if len(data) < 200:
            break

    how_long = time.time() - start_time

    # df_prop=pd.DataFrame(list_prop)
    # df_prop.to_excel(f'realtor-{state_code}-all-properties.xls')
    df_prop = pd.DataFrame(list_prop)
    df_prop.to_csv(f'export/{search_location}-sold.csv', index=False)
    rows = len(df_prop)
    insert_database(df_prop)
    print(f'Finished scraping {rows} recently sold properties from {search_location} in {how_long} seconds \n')


def property_scraper(data):
    dict_prop = {}
    try:
        dict_prop['web_url'] = 'https://www.realtor.com/realestateandhomes-detail/' + data['permalink']
        #print(f"Scraping {dict_prop['web_url']}\n")
    except Exception as e:
        #print('Maximum amount of properties reached\n')
        print(e)
    dict_prop['list_date'] = data['list_date']
    dict_prop['last_update_date'] = data['last_update_date']
    dict_prop['list_price'] = data['list_price']
    dict_prop['status'] = data['status']
    dict_prop['address_line'] = data['location']['address']['line']
    dict_prop['address_city'] = data['location']['address']['city']
    dict_prop['address_state_code'] = data['location']['address']['state_code']
    dict_prop['beds'] = data['description']['beds']
    # dict_prop['baths'] = (data['description']['baths_full'] if data['description']['baths_full'] is not None else 0)+(data['description']['baths_half'] if data[0]['description']['baths_half'] is not None else 0)/2
    dict_prop['baths_full'] = data['description']['baths_full']
    dict_prop['baths_half'] = data['description']['baths_half']
    dict_prop['baths'] = (dict_prop['baths_full'] if dict_prop['baths_full'] is not None else 0) + (dict_prop['baths_half'] if dict_prop['baths_half'] is not None else 0) / 2
    dict_prop['sqft'] = data['description']['sqft']
    dict_prop['year_built'] = str(data['description']['year_built'])
    dict_prop['prop_type'] = data['description']['type']
    dict_prop['sold_date'] = data['description']['sold_date']
    dict_prop['sold_price'] = data['description']['sold_price']
    dict_prop['office_name'] = data['branding'][0]['name']

    # get data from property page
    try:
        prop = requests.get('https://www.realtor.com/realestateandhomes-detail/' + data[
            'permalink']
                       , proxies=proxies
                       , headers=headers1
                      )
        soup = BeautifulSoup(prop.text, 'html.parser')
        next_data = soup.find_all('script', {
            'type': 'application/json'
        })
        jdata = json.loads((next_data[0].text))
        prop = jdata['props']['pageProps']['property']
        dict_prop['last_tax_amount'] = prop['tax_history'][0]['tax'] if prop['tax_history'] is not None else 0
        dict_prop['baths_consolidated'] = prop['description']['baths_consolidated']
        dict_prop['description'] = prop['description']['text']
        # office phone
        dict_prop['office_phone'] = prop['consumer_advertisers'][1]['phone']
        dict_prop['agent_name'] = prop['consumer_advertisers'][0]['name']

    except Exception as e:
        # print(prop.text)
        #print(e)
        #print(i)
        dict_prop['last_tax_amount'] = ''
        dict_prop['baths_consolidated'] = ''
        dict_prop['description'] = ''
        # office phone
        dict_prop['office_phone'] = ''
        dict_prop['agent_name'] = ''

        #print('Captcha found. Trying again ...')

    
    # append to list
    #list_prop.append(dict_prop)
    return dict_prop


def initialize_database():
    database_username = 'XXXXXX'
    database_password = 'XXXXXX'
    database_ip       = 'mysql-portal.XXXXXX.us-east-1.rds.amazonaws.com'
    database_name     = 'realtors'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    database_connection.execute("TRUNCATE TABLE realtors")
    print('Initializing database ...\n')
    
def insert_database(df):
    database_username = 'XXXXXX'
    database_password = 'XXXXXX'
    database_ip       = 'mysql-portal.XXXXXX.us-east-1.rds.amazonaws.com'
    database_name     = 'realtors'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    df.to_sql(con=database_connection, name='realtors', if_exists='append')
        
begin = time.time()

initialize_database()

with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(county_sale,df['search_location'])
    executor.map(county_sold,df['search_location'])

#with concurrent.futures.ThreadPoolExecutor() as executor:
#    executor.map(county_sale,search_location)
#    executor.map(county_sold,search_location)
       
#for loc in search_location:
#    county_sold(loc)

elapsed = time.time() - begin
print(f'Finished all process in {elapsed} seconds')
