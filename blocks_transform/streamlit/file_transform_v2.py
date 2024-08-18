# %%
import pandas as pd
import numpy as np
import json
from geopy.distance import geodesic 
import geopandas as gpd
import math
import streamlit as st
import xlrd


# %%
st.title("Dream Defenders Blocks File Transformer")

# %%
df_zips = pd.read_excel("ZIP_Locale_Detail.xls")
#Reference: https://postalpro.usps.com/ZIP_Locale_Detail
df_zips_counties = pd.read_csv("Zips and Counties - Sheet1.csv", header=None, names=["data"])
#Reference: https://www.unitedstateszipcodes.org/fl/
df_lon_lat = pd.read_csv("uszips.csv")
#Reference: https://simplemaps.com/data/us-zips


def zips(i):
    y = str(i).rstrip('.0')
    if len(y) == 3:
        x = ''.join(("00",y))
    elif len(y) == 4:
        x = ''.join(("0",y))
    elif len(y) == 2:
        x = ''.join((y,"000"))
    elif len(y) == 1:
        x = ''.join((y,"0000"))
    else:
        x = y
    return x
df_zips["PHYSICAL ZIP_CHANGE"] = df_zips["PHYSICAL ZIP"].apply(lambda x: zips(x))

def zips_(i):
    y = str(i).rstrip('.0')
    if len(y) == 3:
        x = ''.join((y,"00"))
    elif len(y) == 4:
        x = ''.join((y,"0"))
    elif len(y) == 2:
        x = ''.join((y,"000"))
    elif len(y) == 1:
        x = ''.join((y,"0000"))
    else:
        x = y
    return str(x)

def concat(x,y):
    if pd.isna(x):
        x = 0
    if pd.isna(y):
        y = 0
    return f"{x},{y}"

def dist(x, y):
    return round(geodesic(eval(x),eval(y)).miles,1)

df_lon_lat['location']=df_lon_lat.apply(lambda row: concat(row['lat'], row['lng']), axis=1)
df_lon_lat['zip']=df_lon_lat['zip'].astype(str)

uploaded_file = st.file_uploader("Choose a file", type = 'csv')
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file, index_col=0)
    df['voting_zip_change']=df['voting_zipcode'].apply(lambda x: zips_(x))
    df['collection_location_zip_change']=df['collection_location_zip'].apply(lambda x: zips_(x) )
    df['voting_location'] = df.apply(lambda row: concat(row['voting_address_latitude'], row['voting_address_longitude']), axis=1)
    df = pd.merge(df,df_lon_lat[['zip','location']],left_on='voting_zip_change',right_on="zip")
    df['voting_location_modified']=np.where(df['voting_location']=='0,0',df['location'],df['voting_location'])
    df['collection_location'] = df.apply(lambda row: concat(row['collection_location_latitude'], row['collection_location_longitude']), axis=1)    
    df['extras'] = df['extras'].apply(lambda x: json.loads(x))
    df = pd.merge(df,df_lon_lat[['zip','location']],left_on='voting_zip_change',right_on="zip")
    df_extras = df.extras.apply(pd.Series)
    df_clean = pd.merge(right=df, left=df_extras, right_index = True, left_index=True)


    df_clean_location = pd.merge(left=df_clean, right=df_zips[["PHYSICAL ZIP_CHANGE", "PHYSICAL CITY"]].drop_duplicates(subset=['PHYSICAL ZIP_CHANGE']), left_on = "voting_zip_change", right_on = "PHYSICAL ZIP_CHANGE",how='left')    
    df_clean_location.rename(columns={"PHYSICAL CITY": "voting_city_clean"}, inplace=True)
    df_clean_location = df_clean_location.drop(["PHYSICAL ZIP_CHANGE"], axis=1)    
    df_clean_location_ = pd.merge(left=df_clean_location, right=df_zips[["PHYSICAL ZIP_CHANGE", "PHYSICAL CITY"]].drop_duplicates(subset=['PHYSICAL ZIP_CHANGE']), left_on = "collection_location_zip_change", right_on = "PHYSICAL ZIP_CHANGE", how="left")    
    df_clean_location_.rename(columns={"PHYSICAL CITY": "collection_city_clean"}, inplace=True)
    df_clean_location_["distance"] = df_clean_location_.apply(lambda row: dist(row['voting_location_modified'], row['collection_location']), axis=1)    
    gender_columns = [col for col in df_clean_location_.columns if 'gender' in col]
    df_clean_location_['gender_combined'] = df_clean_location_[gender_columns].bfill(axis=1).iloc[:, 0]
    race_columns = [col for col in df_clean_location_.columns if 'race' in col]
    df_clean_location_['race_combined']=df_clean_location_[race_columns].bfill(axis=1).iloc[:, 0]
    birth_combined = [col for col in df_clean_location_.columns if 'birth' in col]
    df_clean_location_['birthplace_combined']=df_clean_location_[birth_combined].bfill(axis=1).iloc[:, 0]
    df_clean_location_['gender']=df_clean_location_['gender_combined']
    df_clean_location_['race and ethnicity'] = df_clean_location_['race_combined']
    df_clean_location_['birthplace'] = df_clean_location_['birthplace_combined']
    df_clean_location_.drop(columns=['gender_combined', 'race_combined','birthplace_combined'], inplace=True)

    df_zips_counties['zips_filter']=df_zips_counties['data'].str.isnumeric()
    df_zips_counties['counties_filter']=df_zips_counties['data'].str.contains(" County")

    zips = df_zips_counties[df_zips_counties['zips_filter']==True]
    counties = df_zips_counties[df_zips_counties['counties_filter']==True]

    zips.reset_index(drop=True, inplace=True)
    counties.reset_index(drop=True, inplace=True)
    df_zips_counties_combined = pd.concat([zips,counties], axis=1)

    df_zips_counties_combined.columns.values[0]='zip_code'
    df_zips_counties_combined.columns.values[3]='county'

    df_clean_location__ = pd.merge(left=df_clean_location_, right=df_zips_counties_combined, left_on = "voting_zip_change", right_on = "zip_code",how='left')
    df_clean_location__.rename(columns={"county": "voting_county"}, inplace=True)
    df_clean_location__.rename(columns={'PHYSICAL ZIP_CHANGE':'PHYSICAL_ZIP'}, inplace=True)

    df_clean_location__['voting_street_address_one'] = df_clean_location__['voting_county']
    df_clean_location__['voting_zipcode'] = df_clean_location__['voting_zip_change']
    df_clean_location__['voting_location'] = df_clean_location__['voting_location_modified']
    df_clean_location__['collection_location_zip'] = df_clean_location__['collection_location_zip_change']

    df_clean_location__.drop(columns=[
    'zip_code', 'zips_filter',
        'counties_filter', 'voting_county', 'zips_filter', 'counties_filter','zip_x','location_x',
        'zip_y','location_y','voting_location_modified','collection_location_zip_change','voting_zip_change'], inplace=True)
    
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')
    
    csv = convert_df(df_clean_location__)
    st.download_button("Download Transformed File",
                        csv, 
                        'clean_blocks.csv', 
                        'text/csv', 
                        key='download-csv')



# %%
