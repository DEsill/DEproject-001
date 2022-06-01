from dotenv import load_dotenv 
import os
import pymysql
import pandas as pd
import requests

load_dotenv()
class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)

with connection.cursor() as cursor:
    cursor.execute("SELECT Book_ID, Price FROM audible_data")
    result = cursor.fetchall()
    cursor.execute("SELECT * FROM audible_transaction")
    result1 = cursor.fetchall()

audible_data = pd.DataFrame(result)
audible_data = audible_data.set_index('Book_ID')
audible_transaction = pd.DataFrame(result1)
transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
r = requests.get(url)
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate = conversion_rate.reset_index().rename(columns={"index":"date"})

transaction['date'] = transaction['timestamp']
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

final_df = transaction.merge(conversion_rate, how="left", right_on="date", left_on="date")
final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$", ""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)
final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
final_df = final_df.drop("date", axis=1)

final_df.to_csv("output.csv", index=False)