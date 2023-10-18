from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
import pandas as pd
from config import ACCESS_TOKEN_EXPIRE_MINUTES
from database_conn import database
from helper import authenticate_user, create_access_token, get_current_active_user
from custom_data_type import Token, User, UserInput, SpareRecommendationInput, YardRecommendationInput
# from azure.storage.blob.baseblobservice import BaseBlobService
# from azure.storage.blob import BlobPermissions
from datetime import datetime, timedelta
from azure.storage.blob import ContainerClient
from io import StringIO
import json
import numpy as np
from io import BytesIO

import re
import nltk
nltk.download('punkt')
nltk.download('stopwords')
from nltk.stem import PorterStemmer
from Levenshtein import seqratio
from nltk.tokenize import word_tokenize
from functools import reduce

#connection
container_client = ContainerClient.from_connection_string(
    'DefaultEndpointsProtocol=https;AccountName=treewiseblobstorage;AccountKey=jE3f/ogf+EH2cZyJEEagULdbWrIFvtKOnJB655pvrSn+9jzniIx8hGjHlBvnb3Py2I6h7b5zO2NO+AStfk0NPA==;EndpointSuffix=core.windows.net', container_name='maersk-vendor-recommendation-db')



#use this while quering through pandas
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
# connection_url = URL.create(
#     "mssql+pyodbc",
#     username=username,
#     password=password,
#     host=server,
#     database=server_database,
#     query={"driver": "ODBC Driver 17 for SQL Server"},    
# )
# engine = create_engine(connection_url)

# #read query from sql file
# query = '''
# SELECT		 DISTINCT 
# 				 [Po_Code]					= [Pohd].[Code]
# 				,[Vendor_Id]				= [Pohd].[Vendor_Id]
# 				,[Vendor]					= [Vend].[Name]
# 				,[Vessel_Object_Id]			= [Pohd].[Vessel_Object_Id] 
# 				,[Vessel]					= [VReg].[Name]
# 				,[Vessel_Type]				= [vsl_types].[Name]
# 				,[Sub_Type]					= [sub_types].[Name]
# 				,[Length]					= CONVERT(DECIMAL(18,3), CAST(REPLACE(REPLACE(REPLACE(REPLACE([Part].[Loa],'M',''), 'ETERS', ''),'TRS',''),',','.') AS FLOAT)) 
# 				,[Breadth]					= CONVERT(DECIMAL(18,3),CAST(REPLACE(REPLACE(REPLACE(REPLACE([Breadth],'M',''), 'ETERS', ''),'TRS',''),',','.') AS FLOAT))
# 				,[Depth]					= IIF([Depth] = N'23..2', 23.2, TRY_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([Depth],'M',''), 'ETERS', ''),'TRS',''),',','.'),'..','.') AS FLOAT))
# 				,[Teu]						= CONVERT(INT,[VReg].[Teu])
# 				,[Grt]						= CONVERT(INT,[VReg].[Grt])

# 				,[Item_Id]					= [Podt].[Item_Id] 
# 				,[Item_Category]			= [Cat].[Name]
# 				,[Item_Name]				= [Itm].[Description]
# 				,[Priority]					= [Pri].[Name]
# 				,[Created_On_Year]			= YEAR([Pohd].[Created_Date])
# 				,[Po_Currency]				= [Cur].[Name]
# 				,[Reporting_Currency]		= [Cur1].[Name]
# 				,[Po_Approved_Date]			= CONVERT(DATE, [Pohd].[Approved_Date])
# 				,[Po_Sent_Date]				= CONVERT(DATE, [Iss].[Sent_Date])
# 				,[Delivery_Port]			= [Port].[Name]
# 				,[Uom]						= [Uom].[Name]
# 				,[Po_Unit_Price]			= [Podt].[Unit_Price]
# 				,[Total_Po_Currency]		= [Podt].[Total_Po_Currency]
# 				,[Grn_Prepared_Date]		= CONVERT(DATE,[Grhd].[Prepared_On])
# 				,[Grn_Rating]				= [Grdt].[Rating_Id]
# 				,[Lead_Time]				= IIF ([Iss].[Sent_Date] IS NOT NULL, DATEDIFF(D, [Pohd].[Approved_Date], [Iss].[Sent_Date]), 0)
# 				--,[Po_Qty]					= [Podt].[Quantity]
# 				--,[Accepted_Qty]			= [Grdt].[Accepted_Qty]
# 				--,[Item_Code]				= [Itm].[Code]

# 	FROM		[PAL].[Purchase].[Sp_Po_Hd] [Pohd]
# 	JOIN		[PAL].[Purchase].[Sp_Po_Dt] [Podt]				ON [Podt].[Po_Id]	= [Pohd].[Id] 
# 	JOIN		[PAL].[Common].[Co_Vessel_Register] [VReg]		ON [VReg].[Id]		= [Pohd].[Vessel_Id] AND [VReg].[Object_Id] = [Pohd].[Vessel_Object_Id]
# 	JOIN		[PAL].[Common].[Co_Vessel_Particulars] [part]		ON [Part].[Id]		= [vreg].[object_id]
# 	JOIN		[PAL].[Common].[Co_Admin_Structure] [vsl_types]	ON [vsl_types].[id]	= [vreg].[vessel_type_id]
# 	JOIN		[PAL].[Common].[Co_Admin_Structure] [sub_types]	ON [sub_types].[id]	= [vreg].[vessel_sub_type_id]

# 	JOIN		[PAL].[Common].[Sp_Item_Mf] [Itm]				ON [Itm].[Id]		= [Podt].[Item_Id] 
# 	JOIN		[PAL].[Common].[Sp_Admin_Structure] [Cat]		ON [Cat].[Id]		= [Itm].[Code_Category_Id]
# 	JOIN		[PAL].[Common].[Co_Companies] [Vend]			ON [Vend].[Id]		= [Pohd].[Vendor_Id] 
# 	JOIN		[PAL].[Common].[Co_Currencies] [Cur]			ON [Cur].[Id]		= [Pohd].[Po_Currency_Id] 
# 	JOIN		[PAL].[Common].[Co_Currencies] [Cur1]			ON [Cur1].[Id]		= [Pohd].[Group_Currency_Id] 
# 	JOIN		[PAL].[Common].[Sp_Admin_Structure] [Pri]		ON [Pri].[Id]		= [Pohd].[Priority_Id]
# 	JOIN		[PAL].[Common].[Co_Uom_Mf] [Uom]				ON [Uom].[Id]		= [Podt].[Uom_Id]
# 	LEFT JOIN	[PAL].[Purchase].[Sp_Po_Issued] [Iss]			ON [Iss].[Po_Id]	= [Pohd].[Id] 
# 	LEFT JOIN	[PAL].[Purchase].[SP_PO_Delivery] [Del]		ON [Del].[Po_Id]	= [Pohd].[Id]
# 	LEFT JOIN	[PAL].[Common].[Co_Ports] [Port]				ON [Port].[Id]		= [Del].[Port_Id]
# 	LEFT JOIN	[PAL].[Purchase].[Sp_Goods_Receipt_Hd] [Grhd]	ON [Grhd].[Po_Id]	= [Pohd].[Id]
# 	LEFT JOIN	[PAL].[Purchase].[Sp_Goods_Receipt_Dt] [Grdt]	ON (
# 																		  [Grdt].[Goods_Receipt_Id] = [Grhd].[Id]
# 																	  AND [Grdt].[Po_Dt_Id]			= [Podt].[Id]
# 																	)
# 	LEFT JOIN	[PAL].[Common].[Sp_Admin_Structure] [Rat]		ON [Rat].[Id]		= [Grdt].[Rating_Id]
# 	WHERE		[Itm].[Code]  <> '-1'
#   --AND			YEAR([Pohd].[Created_Date])  = 2022
# 	AND			[Itm].[Description] NOT LIKE '%FEES%'
# '''

# spares_query = '''
# SELECT   JO.[Equipment_Id]
#          ,JO.[Vessel_Object_Id]
#          ,JO.[Job_Plan_Id]
#          ,SP.[Item_Id]
#          ,SP.[Quantity_Consumed]
#          ,YEAR(JO.[Job_End_Date]) AS Job_Year
# FROM     PAL.PMS.PM_JOB_ORDER JO
# JOIN     PAL.PMS.PM_JOB_ORDER_SPARES SP ON JO.ID= SP.JOB_ORDER_ID
# WHERE    YEAR(JO.JOB_END_DATE)>=2017
# --AND    SP.QUANTITY_CONSUMED<>0
# ORDER BY YEAR(JO.[Job_End_Date]), JO.[Equipment_Id] DESC
# '''

# dry_dock_query = '''
# SELECT	 [Project_Id]		= [JItm].[Project_Id]
# 		,[Project]			= [Prj].[Name]
# 		,[Project_Status]	= [Pstat].[Name]

# 		,[Dock_Date]		= CONVERT(DATE, [Prj].[Dock_Date])
# 		,[UnDock_Date]		= CONVERT(DATE, [Prj].[UnDock_Date])
# 		,[Type]				= [JItm].[Type]
# 		,[Vessel_Object_Id]	= [JItm].[Vessel_Object_Id]
# 		,[Vessel]			= [Vreg].[Name] 
# 		,[Vessel_Type]		= [Vsl_Types].[Name]
# 		,[Sub_Type]			= [Sub_Types].[Name]
# 		,[Length]			= CONVERT(DECIMAL(18,3), CAST(REPLACE(REPLACE(REPLACE(REPLACE([Part].[Loa],'M',''), 'ETERS', ''),'TRS',''),',','.') AS FLOAT)) 
# 		,[Breadth]			= CONVERT(DECIMAL(18,3), CAST(REPLACE(REPLACE(REPLACE(REPLACE([Part].[Breadth],'M',''), 'ETERS', ''),'TRS',''),',','.') AS FLOAT))
# 		,[Depth]			= IIF([Depth] = N'23..2', 23.2, TRY_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([Depth],'M',''), 'ETERS', ''),'TRS',''),',','.'),'..','.') AS FLOAT))
# 		,[Teu]				= CONVERT(INT, [VReg].[Teu])
# 		,[Grt]				= CONVERT(INT, [VReg].[Grt])
# 		,[Section]			= [SecM].[Name]
# 		,[Job_Item_Id]		= [JItm].[Id]
# 		,[Job_Code]		    = [JItm].[Code] 
# 		,[Job_Name]			= [JItm].[Title]
# 		,[Assign_Code_Id]	= ISNULL([JItm].[Assign_Code_Id], -1)
# 		,[Assign_Codes]		= [ACods].[Description]	
# 		,[Job_Status_Id]	= ISNULL([JItm].[Job_Status_Id], -1) 
# 		,[Job_Status]		= [Jstat].[Name]
# 		,[Yard_Id]			= [Yard].[Id]
# 		,[Yard]				= [Yard].[Name]
# 	    ,[Yard_Currency]	= [Cur].[Code]
# 		,[Owner_Currency]	= [Cur1].[Code]

# 		,[Estimated_Yard_Cost]		= [Est].[Estimated_Yard_Cost]
# 		,[Add_Or_Adjust]			= [Est].[Add_Or_Adjust]
# 		,[Total_Yard_Cost]			= [Est].[Total_Yard_Cost]


# FROM	[PAL].[DryDock].[Pj_Job_Items] [JItm]
# JOIN	[PAL].[Common].[Pm_Project] [Prj]				ON [Prj].[Id] = [JItm].[Project_Id]
# JOIN	[PAL].[Common].[Co_Vessel_Register] [Vreg]		ON (	
# 																[Vreg].[Id]			= [JItm].[Vessel_Id]
# 															AND [Vreg].[Object_Id]  = [JItm].[Vessel_Object_Id]
# 															)
# JOIN	[PAL].[Common].[Co_Vessel_Particulars] [part]	ON	[Part].[Id]				= [vreg].[object_id]
# JOIN	[PAL].[Common].[PM_STATUS_LIST] [Jstat]			ON	[Jstat].[Id]			= ISNULL([JItm].[Job_Status_Id],-1)
# JOIN	[PAL].[Common].[PM_STATUS_LIST] [Pstat]			ON	[Pstat].[Id]			= ISNULL([Prj].[Project_Status_Id],-1)
# JOIN	[PAL].[Common].[PJ_ASSIGN_CODES] [ACods]		ON	[ACods].[Id]			= ISNULL([JItm].[Assign_Code_Id],-1)
# JOIN	[PAL].[Common].[Co_Admin_Structure] [Vsl_Types]	ON	[Vsl_Types].[Id]		= [vreg].[vessel_type_id]
# JOIN	[PAL].[Common].[Co_Admin_Structure] [Sub_Types]	ON	[Sub_Types].[Id]		= [vreg].[vessel_sub_type_id]
# JOIN	[PAL].[Common].[PJ_Sections_MF] [SecM]			ON	[SecM].[Id]				= [JItm].[Section_Id]
# JOIN	[PAL].[DryDock].[PJ_Job_Cost_Estimate] [Est]    ON	[Est].[Job_Item_Id]		= [JItm].[Id]
# JOIN	[PAL].[Common].[Co_Currencies] [Cur]		   ON  [Cur].[Id]			= [Est].[Yard_Currency_Id]
# JOIN	[PAL].[Common].[Co_Currencies] [Cur1]		   ON  [Cur1].[Id]			= [Est].[Owner_Currency_Id]
# JOIN	[PAL].[Common].[Co_Companies] [Yard]		   ON	[Yard].[Id]			= [Est].[Yard_Id]
# '''

# item_code = '03125480'
# delivery_port = 'Singapore'
lead_time_weight = 0.35
price_weight = 0.35
rating_weight = 0.3



app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def database_connect():
    await database.connect()
@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()


# Authentication
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/recommend_vendor")
async def fetch_data(userinput: UserInput, current_user: User = Depends(get_current_active_user)):
    #print(userinput.dict())
    user_inp =  userinput.dict()
    print(user_inp)
    # data = read_data_from_blob_parquet("export")
    data = read_data_from_blob("Item_Vendor_standardized_data_me_withfirstpart.csv")
    data.dropna(subset=['Po_Unit_Price','Delivery_Port_Id','Delivery_Port'],inplace=True)
    print(data.head(), data.shape)
    #data['Delivery_Port_Id'] = data['Delivery_Port_Id'].astype('int')
    print("data len==",len(data))
    print(data.columns.to_list())
    data = filter_data(data)
    print("filtered data len==",len(data))
    if int(len(data)) == 0:
        raise HTTPException(status_code=404, detail='No data found for the given input')
    try:
        recommended_vendor = recommend_vendor(data, user_inp['eqp_name'], user_inp["item_name"],user_inp['maker'], user_inp['model'], user_inp['part_num'], user_inp['draw_num'], user_inp['pos_num'], user_inp['delivery_port_list'], lead_time_weight, price_weight, rating_weight,user_inp['n_vendor'])
        recommended_vendor.replace({'NA':0,'No data available at this port':0},inplace=True)
        df = pd.DataFrame(recommended_vendor)
        grouped_df = df.groupby(['Vendor','Delivery_Port_Id','Delivery_Port'])[['Item','Score']].agg(list).reset_index()
        # grouped_df['Item_Id'] = grouped_df['Item_Id'].apply(lambda x: list(x))
        # grouped_df['Score'] = grouped_df['Score'].apply(lambda x: list(x))
        print('xxxxxxxxxxxxxxxxxxxxxxxx')
        print(grouped_df)
        try:
            grouped_df['Delivery_Port_Id'] = grouped_df['Delivery_Port_Id'].astype('int')
        except:
            pass    
        # return {'data':{"Vendor_Id": str(grouped_df['Vendor_Id']), "Delivery_Port_Id": str(grouped_df['Delivery_Port_Id']),"Item_Id": str(grouped_df['Item_Id']) ,"Score": str(grouped_df['Score'])}}
        return {'data': {'VendorDeatil':convert_dict(grouped_df.to_dict(orient='records'))}}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail='No data found for the given input')


# def convert_dict(data):
#     data_new = []
#     for entry in data:
#         item_detail = []
#         item_id_set = set(entry["Item_Id"])
#         for item_id in item_id_set:
#             indices = [i for i, x in enumerate(entry["Item_Id"]) if x == item_id]
#             avg_score = sum([entry["Score"][i] for i in indices]) / len(indices)
            
#             item_detail.append({
#                 "Item_Id": item_id,
#                 "Score": avg_score
#             })
#         entry["Item_Detail"] = item_detail
#         entry.pop("Item_Id")
#         entry.pop("Score")
#         entry['vendor_item_count'] = len(entry['Item_Detail'])
#         entry['vendor_item_score'] = sum([i['Score'] for i in entry["Item_Detail"]])
#         data_new.append(entry)
#     return data_new

def convert_dict(data):
    data_new = []
    for entry in data:
        item_detail = []
        item_id_set = set(entry["Item"])
        for item_id in item_id_set:
            indices = [i for i, x in enumerate(entry["Item"]) if x == item_id]
            avg_score = sum([entry["Score"][i] for i in indices]) / len(indices)
            
            item_detail.append({
                "Item": item_id,
                "Score": avg_score
            })
        entry["Item_Detail"] = item_detail
        entry.pop("Item")
        entry.pop("Score")
        entry['vendor_item_count'] = len(entry['Item_Detail'])
        entry['vendor_item_score'] = sum([i['Score'] for i in entry["Item_Detail"]])
        data_new.append(entry)
    return data_new

def query_database(query):
    query_df = pd.read_sql_query(sql=query, con=engine)
    return query_df


def read_data_from_blob(dataset_name):
    try:
        data = pd.read_csv(StringIO(container_client.download_blob(dataset_name).content_as_text()))
        return data
    except Exception as e:
        print("EXcception in reading from BLOB",e)
        raise HTTPException(status_code=404, detail='Error in reading data from blob storage')


def read_data_from_blob_parquet(dataset_name):
    try:
        
        blob_client = container_client.get_blob_client(blob=dataset_name)
        stream_downloader = blob_client.download_blob()
        stream = BytesIO()
        stream_downloader.readinto(stream)
        processed_df = pd.read_parquet(stream, engine='fastparquet')
        return processed_df
    except Exception as e:
        print("EXcception in reading from BLOB",e)
        raise HTTPException(status_code=404, detail='Error in reading data from blob storage')

# def read_data_from_blob_parquet(dataset_name):
#     try:
#         data = pd.read_parquet(StringIO(container_client.download_blob(dataset_name).content_as_text()),engine='fastparquet')
#         # data=pd.read_parquet('export', engine='fastparquet')
#         return data
#     except Exception as e:
#         print("EXcception in reading from BLOB",e)
#         raise HTTPException(status_code=404, detail='Error in reading data from blob storage')


# def recommend_vendor(data, item_code_list, delivery_port_list, lead_time_weight, price_weight, rating_weight,n_vendor):
#     final_dataframe=pd.DataFrame(columns=['Vendor_Id','Vendor_Code','Vendor','Item_Id','Delivery_Port_Id','Delivery_Port','Score'])
#     # Looping through the various delivery ports provided by the user
#     for deliveryport in delivery_port_list:
#         #print("deliveryport",deliveryport)
#         #  Looping through the various items provided by the user
#         for item in item_code_list:
#             #print("item================",item)
#             filtered_data = data[(data['Item_Id'] == item) & (data['Delivery_Port_Id'] == deliveryport)]
#             #print("filtereddata",filtered_data.head())
#             #print("filtered data len for given query ==",len(filtered_data))
#             if len(filtered_data) == 0:
#                 print('xxxxxxxxxxx')
#                 print("'No data found for the given input'")
#                 continue
#             # Normalize lead time, price, and rating
#             #print("filtered_data['Lead_Time'].max() ==",filtered_data['Lead_Time'].max())
#             min_l=filtered_data['Lead_Time'].min()
#             max_l=filtered_data['Lead_Time'].max()
#             filtered_data['Normalized_Lead_Time'] = filtered_data['Lead_Time'].apply(lambda x: normalize(x, min_l,max_l))
#             min_p=filtered_data['Po_Unit_Price'].min()
#             max_p=filtered_data['Po_Unit_Price'].max()
#             #print(filtered_data.info())
#             # print('min_p',min_p)
#             # print('max_p',max_p)
#             #print("------------------------------------------------------")
#             filtered_data['Normalized_Price'] = filtered_data['Po_Unit_Price'].apply(lambda x: normalize(x, min_p,max_p))
#             filtered_data['Normalized_Rating'] = filtered_data['Rating'].apply(lambda x: normalize(x, 0,4))
#             #print("filtered dataframe\n",filtered_data.head())
#     # Calculate the weighted score
            
#             filtered_data['Score'] = lead_time_weight * filtered_data['Normalized_Lead_Time'] + price_weight * filtered_data['Normalized_Price']+ rating_weight * filtered_data['Normalized_Rating']
#             filtered_data = filtered_data.groupby(['Vendor_Id','Vendor_Code','Vendor','Item_Id','Delivery_Port_Id', 'Delivery_Port'])['Score'].agg('mean').reset_index()
#             filtered_data.reset_index(drop=True, inplace=True)
#             #print("filtered_data====================================",filtered_data)
            
#     # reorder the dataframe according to rank (descending)
            
#             recommended_vendor = filtered_data.sort_values(by='Score', ascending=False)
            
#     # select the top n numner of vendors from data frame
#             filtered_reco=recommended_vendor.head(n_vendor)
#             filtered_reco=filtered_reco[['Vendor_Id','Vendor_Code','Vendor','Item_Id','Delivery_Port_Id','Delivery_Port','Score']]
#             #print("{} \n recommended vendor before appedn===========================\n{}\n{}".format(len(filtered_reco),pd.DataFrame(filtered_reco),final_dataframe))
#             if len(filtered_reco)!=0:
#                 final_dataframe= pd.concat([final_dataframe,filtered_reco])
#             # Need to pivot the dataframe
#             print(" ******************final dataframe***********************\n",final_dataframe)
#     #print("---------------------------------------------\n",final_dataframe)
#     return final_dataframe
def recommend_vendor(data, eqp_name,item_name, maker, model, part_num, draw_num, pos_num, delivery_port_list, lead_time_weight, price_weight, rating_weight,n_vendor):
    # final_dataframe=pd.DataFrame(columns=['Vendor_Id','Vendor_Code','Vendor','Item_Id','Delivery_Port_Id','Delivery_Port','Score'])
    final_dataframe=pd.DataFrame(columns=['Item','Vendor','Delivery_Port_Id','Delivery_Port','Score'])
    print('started')
    ps = PorterStemmer()
    items_id_map = {}
    new_dict = {}
    cat = [' '.join(list(j.split())[:-1]) for j in data['Item_Mapps_id'].unique()]
    for eq,it,mk,md,par,drw,pos in zip(eqp_name,item_name, maker, model, part_num, draw_num, pos_num):
        ids = Item_id_gen_sub(eq,it,mk,md,par,drw,pos,ps)
        try:
            items_id_map.update(ids)
            print('has global')
        except:
            # print('ids',ids)
            # print('item_name',it)
            # print('pos_num',pos)
            first_part, second_part, last_part, status= ids[0], ids[1], it+'_'+pos, ids[2]#first part : full part
            if status==True:
                print('sim checking')
                print('it -',it)
                
                main_dict = {}
                for y in cat:
                    main_dict[y] = seqratio(first_part,y)
                replace = {}
                list1 = []
                for key in main_dict.keys():
                    s_dict = {k: v for k,v in main_dict.items() if v>.80} #80%   
                    s_dict_filt = dict(sorted(s_dict.items(),key=lambda x:x[1],reverse=True))
                    try:
                        del s_dict_filt[key]
                    except:
                        pass    
                    if len(s_dict_filt.keys())>0:
                        items_id_map[first_part+' '+second_part] = [list(s_dict_filt.keys())[0]+' '+second_part, last_part]
                    else:
                        items_id_map[first_part+' '+second_part]  = [None,last_part]               
                print('reached')     
            else:
                final_dataframe= pd.concat([final_dataframe,pd.DataFrame({'Item':it,'Vendor':'NA','Delivery_Port_Id':'NA','Delivery_Port':'NA','Score':'No data available at this port'},index=[0])])    
  
    print('items_id_map',items_id_map)           
    for keyys in items_id_map:
        if items_id_map[keyys][0]!=None:
            if items_id_map[keyys][0] in data['Item_Mapps_id'].values:
                cp = items_id_map[keyys][0]
                filtered_data = data[data['Item_Mapps_id'] == cp]
                try:
                    filtered_data['Delivery_Port_Id'] = filtered_data['Delivery_Port_Id'].astype(int)
                except:
                    pass    
                print('port id',filtered_data['Delivery_Port_Id'])
                for deliveryport in [delivery_port_list]:
                    filtered_data1 = filtered_data[filtered_data['Delivery_Port_Id'] == deliveryport]
                    print('del len - ',len(filtered_data1))
                    if len(filtered_data1) == 0:
                        print('xxxxxxxxxxx')
                        print("'No data found for the given input'")
                        final_dataframe= pd.concat([final_dataframe,pd.DataFrame({'Item':list(items_id_map[keyys][1].split('_'))[0],'Vendor':'NA','Delivery_Port_Id':'NA','Delivery_Port':deliveryport,'Score':'No data available at this port'},index=[0])]) #['Item','Vendor','Delivery_Port_Id','Delivery_Port','Score']
                        # filtered_reco=filtered_reco[['Vendor_Id','Vendor_Code','Vendor','Item_Id','Delivery_Port_Id','Delivery_Port','Score']]
                        continue
                    min_l=filtered_data1['Lead_Time'].min()
                    max_l=filtered_data1['Lead_Time'].max()
                    filtered_data1['Normalized_Lead_Time'] = filtered_data1['Lead_Time'].apply(lambda x: normalize(x, min_l,max_l))
                    min_p=filtered_data1['Po_Unit_Price'].min()
                    max_p=filtered_data1['Po_Unit_Price'].max()  
                    
                    filtered_data1['Normalized_Price'] = filtered_data1['Po_Unit_Price'].apply(lambda x: normalize(x, min_p,max_p))
                    filtered_data1['Normalized_Rating'] = filtered_data1['Rating'].apply(lambda x: normalize(x, 0,4))
                    
                    filtered_data1['Score'] = lead_time_weight * filtered_data1['Normalized_Lead_Time'] + price_weight * filtered_data1['Normalized_Price']+ rating_weight * filtered_data1['Normalized_Rating']
                    # filtered_data1 = filtered_data1.groupby(['Vendor_Id','Vendor_Code','Vendor','Item_Id','Delivery_Port_Id', 'Delivery_Port'])['Score'].agg('mean').reset_index()
                    filtered_data1 = filtered_data1.groupby(['Item','Vendor','Delivery_Port_Id','Delivery_Port'])['Score'].agg('mean').reset_index()
                    filtered_data1.reset_index(drop=True, inplace=True)
                    
                    recommended_vendor = filtered_data1.sort_values(by='Score', ascending=False)
                    
                    filtered_reco=recommended_vendor.head(n_vendor)
                    filtered_reco=filtered_reco[['Item','Vendor','Delivery_Port_Id','Delivery_Port','Score']]
                    print('filtered_reco',filtered_reco)
                    # filtered_reco.loc[0,'Item'] = list(items_id_map[keyys][1].split('_'))[0]
                    
                    if len(filtered_reco)!=0:
                        final_dataframe= pd.concat([final_dataframe,filtered_reco])
                    print(" ******************final dataframe***********************\n",final_dataframe) 
            else:
                final_dataframe= pd.concat([final_dataframe,pd.DataFrame({'Item':list(items_id_map[keyys][1].split('_'))[0],'Vendor':'NA','Delivery_Port_Id':'NA','Delivery_Port':'NA','Score':'No data available at this port'},index=[0])])                       
        else:
            final_dataframe= pd.concat([final_dataframe,pd.DataFrame({'Item':list(items_id_map[keyys][1].split('_'))[0],'Vendor':'NA','Delivery_Port_Id':'NA','Delivery_Port':'NA','Score':'No data available at this port'},index=[0])])                      
        print('final_dataframe -',final_dataframe)
    return final_dataframe
# recommend_vendor(data, ['main engin fuel pump','main engin fuel injector'],['O-RING','SPRING'], ['< MITSUBISHI >','< MAN B&W >'], ['< UEC43LSII >','< S35MC7 >'], ['1411182200','207'], ['239254000000','P909100152'], ['20','207'], 2661, lead_time_weight, price_weight, rating_weight,2)
# def filter_data(data):
#     try:
#         data=data[data['Lead_Time']>0]
#         data=data[data['Po_Unit_Price']>0]
#         data=data[data['Po_Unit_Price']>0]
#         # mapping = {'Unsatisfactory-Do Not Pay':4, 'Average-Ok to Pay':3, 'Good-Ok to Pay':2, 'Very Good-Ok to Pay':1,\
#         #                 'Excellent-Ok to Pay':0}
#         data['Grn_Rating'] = data['Grn_Rating'].fillna(0)
#         mapping = { 3888:2,9643:0,9644:3,9645:4,9646:1,0:3}
#         # Use map to replace values in the 'Column' column
#         data['Rating'] = data['Grn_Rating'].map(mapping)
#         return data
#     except Exception as e:
#         print(e)
#         raise HTTPException(status_code=404, detail='no data found for this query')
def filter_data(data):
    try:
        # data=pd.read_csv(r"E:\OneDrive - MariApps Marine Solutions Pte.Ltd\Treeswise_projects\Vendor Standardization\Vendor_master\Results_all_clients\Item_Vendor_standardized_data_me_withfirstpart.csv")
        # data = read_data_from_blob("Item_Vendor_standardized_data_me_withfirstpart.csv")
        # data.dropna(subset=[''])  
        data=data[data['Lead_Time']>0]
        # data=data[data['Po_Unit_Price']>0]
        data.dropna(subset=['Po_Unit_Price','Delivery_Port_Id','Delivery_Port'],inplace=True)
        data=data[data['Po_Unit_Price']>0]
        # mapping = {'Unsatisfactory-Do Not Pay':4, 'Average-Ok to Pay':3, 'Good-Ok to Pay':2, 'Very Good-Ok to Pay':1,\
        #                 'Excellent-Ok to Pay':0}
        data['Grn_Rating'] = data['Grn_Rating'].fillna(0)
        mapping = { 3888:2,9643:0,9644:3,9645:4,9646:1,0:3}
        # Use map to replace values in the 'Column' column
        data['Rating'] = data['Grn_Rating'].map(mapping)
        return data
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail='no data found for this query')
    
def Item_id_gen_sub(eqp,item,mak,mod,par,draw,pos,ps):
    # global_mapped_data = pd.read_csv(r"E:\OneDrive - MariApps Marine Solutions Pte.Ltd\Treeswise_projects\Vendor Standardization\Vendor_master\Results_all_clients\Item_Vendor_standardized_data_me_withfirstpart.csv")
    global_mapped_data = read_data_from_blob("Item_Vendor_standardized_data_me_withfirstpart.csv")
    global_mapped_data.dropna(subset=['Po_Unit_Price','Delivery_Port_Id','Delivery_Port'],inplace=True)
    # ps = PorterStemmer()
    #For processing equipment name
    eqp = re.sub('\W',' ',eqp) 
    eqp = re.sub(r'\s{2,}',' ',eqp).strip().lower()
    eqp = re.sub(r'[nm]o \d+|[nm]\d+|[nm]\d \d+',' ',eqp) #removing NO.1 similar patterns
    eqp = re.sub(r'^\d*\s*|\s*\d*$','',eqp) #remove staring n leading numbers
    eqp = re.sub(r'\s{2,}',' ',eqp).strip()
    token = word_tokenize(eqp) #splitting sentence into words
    stemmed_sentence = reduce(lambda x, y: x + " " + ps.stem(y), token, "") #apply stemming
    eqp = stemmed_sentence.strip()
    
    reg1 = re.sub(r'main engin |me |m e |main engin \d+ |me\d+ |me \d+ ','main engin ',eqp).strip().lower()
    eqp = re.sub(r'\s{2,}',' ',reg1).strip()
    
    item_mak_mod = eqp+' '+item+' '+mak+' '+mod
    item_mak_mod = re.sub('\W',' ',item_mak_mod) 
    item_mak_mod = re.sub(r'\s{2,}',' ',item_mak_mod).strip().lower()
    item_mak_mod = re.sub(r'[nm]o \d+|[nm]\d+|[nm]\d \d+',' ',item_mak_mod) #removing NO.1 similar patterns
    item_mak_mod = re.sub(r'^\d*\s*|\s*\d*$','',item_mak_mod) #remove staring n leading numbers
    item_mak_mod = re.sub(r'\s{2,}',' ',item_mak_mod).strip()
    token2 = word_tokenize(item_mak_mod) #splitting sentence into words
    stemmed_sentence2 = reduce(lambda x, y: x + " " + ps.stem(y), token2, "") #apply stemming
    item_mak_mod = stemmed_sentence2.strip()
    second_part = par+draw+pos
    second_part = re.sub(r'\W','',second_part).lower().strip()
    print('processing done')
    if 'main engin' in item_mak_mod:
        if item_mak_mod in global_mapped_data['item_firstpart_processed'].values:
            print('1')
            yu = list(global_mapped_data[global_mapped_data['item_firstpart_processed']==item_mak_mod]['Item_Mapps_id'])[0]
            first_part_stand = ' '.join(list(yu.split())[:-1])
            print('item_mak_mod -',item_mak_mod)
            print('first_part_stand -',first_part_stand)
            if first_part_stand+' '+second_part in global_mapped_data['Item_Mapps_id'].values:
                print(2)
                item_global_id = list(global_mapped_data[global_mapped_data['Item_Mapps_id']==first_part_stand+' '+second_part]['Item_Mapps_id'])[0]
                print('item_mak_mod',item_mak_mod)
                print('full matching')
                return {first_part_stand+' '+second_part:[item_global_id,item+'_'+pos]}
            else:
                print(3)
                print('item_mak_mod',item_mak_mod)    
                print('first part matching')
                return first_part_stand, second_part, False
        else:
            print(4)
            print('first part not matching')
            print('item_mak_mod',item_mak_mod)
            print('second_part',second_part)
            return item_mak_mod, second_part, True       
            
    else:
        print(5)
        print('item_mak_mod',item_mak_mod)
        print('Provided item not related to main engine')    


def normalize(v,min_l,max_l):
    
    if max_l==min_l:
        return .5
    return  1-((v - min_l) / (max_l - min_l))






###########################################################spare recommendationXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
def recommend_items(dataset, vessel_object_id, job_plan, equipment):
    df = dataset.copy()
    recommendations = {}
    
    similar_jobs = df[(df['Job_Plan'] == job_plan) & (df['Equipment'] == equipment) & (df['Vessel_Object_Id'] == int(vessel_object_id)) & (df['Quantity_Consumed'] > 0)]
    print('similar_jobs :: ', similar_jobs)
    
    if not similar_jobs.empty:
        avg_qty = similar_jobs.groupby('Item')['Quantity_Consumed'].mean().reset_index()
        recommended_items =avg_qty
        #avg_qty[avg_qty['Quantity_Consumed'] > np.median(df['Quantity_Consumed'])]
        
        if not recommended_items.empty:
            #append the items to the recommendations dictionary
            for index, row in recommended_items.iterrows():
                recommendations[row['Item']] = row['Quantity_Consumed']
        else:
            recommendations[job_plan] = "No items with consumed quantity higher than median"
    else:
        recommendations[job_plan] = "No similar job plans found"
    print('reccc  :: ',  recommendations)
    rec_list =[]
    for key, value in recommendations.items():
        rec = {}
        try:
            rec[key] = value
            rec_list.append(rec)
        except:
            rec[key] = value
            rec_list.append(rec)
    print('rec_list :: ', rec_list)
    return rec_list


@app.post("/spare_recommendation")
async def fetch_data(userinput: SpareRecommendationInput, current_user: User = Depends(get_current_active_user)):
    print(userinput.dict())
    print('hello')
    user_inp =  userinput.dict()
    print(user_inp)
    
    data = read_data_from_blob("spare_dataset.csv")
    print("data len==",len(data))
    print(data.columns.to_list())
    try:
        #job_plan_ids = [int(i) for i in user_inp['job_plan_id_list']]
        #print("job_plan_ids==",job_plan_ids)
        spare_recommendation = recommend_items(data, user_inp['vessel_object_id'], user_inp['job_plan'], user_inp['equipment'])
        return {'data': spare_recommendation}
    except Exception as e:
        raise HTTPException(status_code=404, detail='no spare part recommendation found')






#############################################data testinggggggggggggggggggggggggggggg
@app.get("/get_blob_data")
async def get_blob_data():
    try:
        data = read_data_from_blob("dry_dock_dataset.csv")
        data = data.to_json(orient="records")
        data = json.loads(data)
        return data

    except Exception as e:
        raise HTTPException(status_code=404, detail='Error in reading data from blob storage')


def get_dataset(dataset_namre):
    try:
        data =  query_database(dataset_namre)
    except Exception as e:
        try:
            data = read_data_from_blob("spare_dataset.csv")
        except Exception as e:
            print(e)
            raise HTTPException(status_code=404, detail='No data found for the given input')



##############################testingggggggggggggggggggggggg
@app.post("/get_data_test")
async def get_data(dataset_name: str, current_user: User = Depends(get_current_active_user)):
    try:
        data = data = get_dataset(dataset_name)
        data = data.to_json(orient="records")
        data = json.loads(data)
        return data
    except Exception as e:
        raise HTTPException(status_code=404, detail='Error in reading data from blob storage')






################################### YARD RECOMMENDATION#############################################################
@app.post("/yard_recommendation")
async def fetch_data(userinput: YardRecommendationInput, current_user: User = Depends(get_current_active_user)):
    print(userinput.dict())
    user_inp =  userinput.dict()
    print(user_inp)
    
    data = read_data_from_blob("dry_dock_dataset.csv")
    print("data len==",len(data))
    print(data.columns.to_list())
    try:
        best_yard = recommend_best_yard(data, user_inp)
        return {'data' : {"best_yard":best_yard}}
    except Exception as e:
        raise HTTPException(status_code=404, detail='no yard recommendation found')


# Function to recommend the best yard
def recommend_best_yard(ship_data,  features):
    # Filter the ship_data
    filtered_data = ship_data[
        (ship_data['Vessel_Type'] == features['Vessel_Type']) &
        (ship_data['Sub_Type'] == features['Sub_Type']) &
        (ship_data['Length'] >= features['Length'] * 0.5) &
        (ship_data['Length'] <= features['Length'] * 1.5) &
        (ship_data['Breadth'] >= features['Breadth'] * 0.5) &
        (ship_data['Breadth'] <= features['Breadth'] * 1.5) &
        # (ship_data['Depth'] >= features['Depth'] * 0.5) &
        # (ship_data['Depth'] <= features['Depth'] * 1.5) &
        (ship_data['Estimated_Yard_Cost'] != 0)
    ]

    # Count unique sections
    unique_sections_count = ship_data['Section'].nunique()

    # Group the filtered data by Yard and Section (Standard Job) and calculate the mean estimated yard cost
    grouped_data = filtered_data.groupby(['Yard_Id', 'Yard', 'Section'], as_index=False)['Estimated_Yard_Cost'].mean()

    # Calculate the number of covered sections for each yard
    yard_section_coverage = grouped_data.groupby(['Yard_Id', 'Yard'], as_index=False)['Section'].count()

    # Filter yards covering at least 80% of unique sections
    #yard_section_coverage = yard_section_coverage[yard_section_coverage['Section'] >= 0.8 * unique_sections_count]

    # Rank the yards by their average estimated cost (ascending order)
    yard_section_coverage = yard_section_coverage.sort_values('Section')

    # Return the best yard
    return yard_section_coverage.iloc[0]['Yard'] if not yard_section_coverage.empty else None
