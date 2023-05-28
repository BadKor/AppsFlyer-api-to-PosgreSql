import requests
from io import StringIO
import pandas as pd
import pprint
import json
import psycopg2
import csv


def loads_installs(id_af, token,date1,date2):
    url = "https://hq1.appsflyer.com/api/raw-data/export/app/"+id_af+"/installs_report/v5?from="+date1+"&to="+date2+"&reattr=false&additional_fields=blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att"
    headers = {
        "accept": "text/csv",
        "authorization": "Bearer "+token
    }
    #We get data in csv format and we need reformating to json format that load data to PosgreSql
    response_installs = requests.get(url, headers=headers)
    csvStringIO = StringIO(response_installs.text)
    df = pd.read_csv(csvStringIO, sep=",", header=None)
    df = pd.DataFrame(df)

    #Delete first line because for json format in this line we don't want title of colums
    a = df.iloc[0:1]
    a_list = a.values.tolist()
    df = df.drop(df.index[0])
    df.columns = a_list[0]
    df.dropna(axis='columns', how='all', inplace=True)

    #load to json format
    json_table = df.to_json(orient='records')
    json_installs = json.loads(json_table)


    #Connect to DataBase PosgreSql
    conn = psycopg2.connect(dbname="dbname", host="10.10.10.10", user="test", password="test", port="9000")
    print("Connected to DB")
    cursor = conn.cursor()

    # Sql request for insert data to DB, where table_name - it's name your table in PosgreSql
    query_sql = """insert into table_name
                        select * from json_populate_recordset(NULL::table_name, %s) """
    cursor.execute(query_sql, (json.dumps(json_installs),))
    conn.commit()
    conn.close()

def loads_protect360(id_af, token,date1,date2):

    url = "https://hq1.appsflyer.com/api/raw-data/export/app/"+id_af+"/detection/v5?from="+date1+"&to="+date2+"&additional_fields=contributor3_match_type,detection_date,gp_click_time,match_type,gp_broadcast_referrer,gp_install_begin,custom_data,fraud_reason,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,fraud_reasons,fraud_sub_reason,gp_referrer,install_time_tz,device_category,rejected_reason_value,network_account_id,install_app_store,amazon_aid,att"
    headers = {
        "accept": "text/csv",
        "authorization": "Bearer "+token
    }


    #We get data in csv format and we need reformating to json format that load data to PosgreSql
    response_protect360 = requests.get(url, headers=headers)
    csvStringIO = StringIO(response_protect360.text)
    df = pd.read_csv(csvStringIO, sep=",", header=None)
    df = pd.DataFrame(df)


    # Delete first line because for json format in this line we don't want title of colums
    a = df.iloc[0:1]
    a_list = a.values.tolist()
    df=df.drop(df.index[0])
    df.columns = a_list[0]
    df.dropna(axis='columns',how='all', inplace=True)

    # load to json format
    json_table = df.to_json(orient='records')
    json_protect360=json.loads(json_table)

    # Connect to DataBase PosgreSql
    conn = psycopg2.connect(dbname="traffic", host="10.10.11.104", user="traffic", password="1234567", port="5432")
    print("Connected to DB")
    cursor = conn.cursor()

    # Sql request for insert data to DB, where table_name - it's name your table in PosgreSql
    query_sql = """insert into table_name 
                    select * from json_populate_recordset(NULL::table_name, %s) """
    cursor.execute(query_sql, (json.dumps(json_protect360),))
    conn.commit()
    conn.close()

def loads_events(id_af, token, event,date1,date2):

    url = "https://hq1.appsflyer.com/api/raw-data/export/app/"+id_af+"/in_app_events_report/v5?from="+date1+"&to="+date2+"&event_name="+event+"&additional_fields=blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att"
    headers = {
        "accept": "text/csv",
        "authorization": "Bearer "+token
    }


    #We get data in csv format and we need reformating to json format that load data to PosgreSql
    response_events = requests.get(url, headers=headers)
    csvStringIO = StringIO(response_events.text)
    df = pd.read_csv(csvStringIO, sep=",", header=None)
    df = pd.DataFrame(df)


    # Delete first line because for json format in this line we don't want title of colums
    a = df.iloc[0:1]
    a_list = a.values.tolist()
    df=df.drop(df.index[0])
    df.columns = a_list[0]
    df.dropna(axis='columns',how='all', inplace=True)

    # load to json format
    json_table = df.to_json(orient='records')
    json_events=json.loads(json_table)

    # Connect to DataBase PosgreSql
    conn = psycopg2.connect(dbname="traffic", host="10.10.11.104", user="traffic", password="1234567", port="5432")
    print("Connected to DB")
    cursor = conn.cursor()

    # Sql request for insert data to DB, where table_name - it's name your table in PosgreSql
    query_sql = """insert into table_name
                   select * from json_populate_recordset(NULL::table_name, %s) """
    cursor.execute(query_sql, (json.dumps(json_events),))
    conn.commit()
    conn.close()



token = 'YOUR_Token_AppsFlyer'

id_app = 'Your app id'

#For function loads_events. If you want see in your DB not all events, you can send request to api
# who you want see in your API. Because very often in AppsFlyer you saved very more events for example: clicks to button1, button2,3...etc.
# And that not spend more memory for other event you can choose only one, or more events for you table.
# In my example we be send request with one event
event_name ='event12345'

#Date range for load data from api AppsFlyer: date1 start, date2 - end
date1 = '2023-04-29'
date2 = '2023-04-30'

loads_installs(id_af=id_app, token=token,date1=date1,date2=date2)
loads_protect360(id_af=id_app, token=token,date1=date1,date2=date2)
loads_events(id_af=id_app, token=token,event=event_name,date1=date1,date2=date2)
