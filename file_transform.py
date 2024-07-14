# %%
import pandas as pd
import numpy as np
import json
from geopy.distance import geodesic 
import geopandas as gpd
import math

# %%
st.title("Dream Defenders Blocks File Transformer")

# %%
uploaded_file = st.file_uploader("Choose a file", type = 'csv')
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file, index_col=0)
    df_zips = pd.read_excel("ZIP_Locale_Detail.xls")
    df_zips_counties = pd.read_csv("Zips and Counties - Sheet1.csv", header=None, names=["data"])
    
    def zips(i):
    y = str(i)
    if len(y) == 3:
        x = ''.join(("00",y))
    elif len(y) == 4:
        x = ''.join(("0",y))
    else:
        x = y
    return x
    df_zips["PHYSICAL ZIP_CHANGE"] = df_zips["PHYSICAL ZIP"].apply(lambda x: zips(x))

    def concat(x,y):
    if pd.isna(x):
        x = 0
    if pd.isna(y):
        y = 0
    return f"{x},{y}"
    df['voting_location'] = df.apply(lambda row: concat(row['voting_address_latitude'], row['voting_address_longitude']), axis=1)
    df['collection_location'] = df.apply(lambda row: concat(row['collection_location_latitude'], row['collection_location_longitude']), axis=1)

    def dist(x, y):
    return round(geodesic(eval(x),eval(y)).miles,1)
    df_clean_location_["distance"] = df_clean_location_.apply(lambda row: dist(row['voting_location'], row['collection_location']), axis=1)

    df['extras'] = df['extras'].apply(lambda x: json.loads(x))
    df_extras = df.extras.apply(pd.Series)
    df_clean = pd.merge(right=df, left=df_extras, right_index = True, left_index=True)

    df_clean_location = pd.merge(left=df_clean, right=df_zips[["PHYSICAL ZIP", "PHYSICAL CITY"]], left_on = "voting_zipcode", right_on = "PHYSICAL ZIP")
    df_clean_location.rename(columns={"PHYSICAL CITY": "voting_city_clean"}, inplace=True)
    df_clean_location = df_clean_location.drop(["PHYSICAL ZIP"], axis=1)
    df_clean_location_ = pd.merge(left=df_clean_location, right=df_zips[["PHYSICAL ZIP", "PHYSICAL CITY"]], left_on = "collection_location_zip", right_on = "PHYSICAL ZIP")
    df_clean_location_.rename(columns={"PHYSICAL CITY": "collection_city_clean"}, inplace=True)
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
    df_zips_counties_combined['zip_code']=df_zips_counties_combined['zip_code'].astype(float)

    df_clean_location__ = pd.merge(left=df_clean_location_, right=df_zips_counties_combined, left_on = "voting_zipcode", right_on = "zip_code")
    df_clean_location__.rename(columns={"county": "voting_county"}, inplace=True)

    df_clean_location__['voting_street_address_one'] = df_clean_location__['voting_county']

    df_clean_location__.drop(columns=[
    'zip_code', 'zips_filter',
        'counties_filter', 'voting_county', 'zips_filter', 'counties_filter'], inplace=True)
    
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')
    
    csv = convert_df(df_clean__)
    st.download_button("Download Transformed File",
                        csv, 
                        'clean_blocks.csv', 
                        'text/csv', 
                        key='download-csv')


