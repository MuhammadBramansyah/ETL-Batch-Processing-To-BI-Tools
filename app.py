## Mengimport Library yg di inginkan
# data lib
from http import client
from traceback import print_tb
import pandas as pd 
import numpy as np

# json lib
import json

#path lib
import os

#google lib
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#Warnings ignore
import warnings
warnings.filterwarnings('ignore')

if __name__ == "__main__": ## class

    ## load data
    path = os.getcwd() 
    path_data = path + '/data/' ## mendefine lokasi folder data 
    path_connection = path + '/connector/' ## mendinfine lokasi file json (konektor ke GCP and Gsheet)

    filename_order = 'TR_OrderDetails' ## define nama folder source data_1 
    filename_product = 'TR_Products' ## define nama folder source data_2
    filename_property = 'TR_PropertyInfo' ## define nama folder source data_3

    ## load dataset
    df_order = pd.read_csv(path_data + f'{filename_order}.csv') ## load dataset_1
    df_product = pd.read_csv(path_data + f'{filename_product}.csv')  ## load dataset_2
    df_property = pd.read_csv(path_data + f'{filename_property}.csv')  ## load dataset_3

    ## Data Manipulation 
        # new column for property City
    df_order['PropertyCities'] = df_order['PropertyID'] \
                            .map(df_property.set_index('Prop ID')['PropertyCity'].to_dict()) ## membuat column baru(PropertyCities) dengan memasukan value column tersebut dengan membuat tempr dict
        
        # New Column for Product Name
    df_order['ProductName'] = df_order['ProductID'] \
                            .map(df_product.set_index('ProductID')['ProductName'].to_dict()) ## membuat column baru(ProductName) dengan memasukan value column tersebut dengan membuat tempr dict

        # New Column for Price
    df_order['Price'] = df_order['ProductID'] \
                            .map(df_product.set_index('ProductID')['Price'].to_dict()) ## membuat column baru(Price) dengan memasukan value column tersebut dengan membuat tempr dict
        
        # New Column for Sales    
    df_order['Sales'] = df_order['Quantity'] * df_order['Price'] ## membuat column baru(PropertyCities) dengan mengkalkulasi antara column quantity dengan column price

        # Memilihi colum yang ingin di pakai sebagai report
    cols_order = ['OrderID', 'OrderDate','PropertyCities', 'ProductName','Quantity','Price','Sales'] ## memilihi column sesuai dengan kebutuhan pada data warehouse
    df_order = df_order[cols_order] ## memasukan/ mengganti semua column dan valuenya dengan variabel column yg sudah di define diatas
    
        # Konvert Datetime
    df_order['OrderDate'] = pd.to_datetime(df_order['OrderDate']).dt.strftime('%Y-%m-%d') ## mengkonvert tipe data dari object menjadi datetime serta merubah format pada column date
    
    ## connection google API
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive'] ## 
    creds = ServiceAccountCredentials.from_json_keyfile_name(path_connection  + 'zippy-chain-345512-1414d301d9c5.json', scope) ## mendifine credential GCP dengan memasukan path folder koneksi dengan file json yg didapatkan dari GCP
    client = gspread.authorize(creds) ## autorize credential GCP kita

    ## Connection Google Sheet
    file = client.open('data_de6') ## meendifine datawarehouse(google Sheet) yg dituju 
    worksheet = file.worksheet('Sheet4') ## mendifine table yg akan dimasukan datanya.

    ## export Data to Google Sheet
    worksheet.update([df_order.columns.values.tolist()] + df_order.values.tolist()) ## load data yg sudah di transform/diolah ke table yg dituju





