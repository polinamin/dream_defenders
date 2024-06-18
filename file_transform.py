# %%
import streamlit as st
import pandas as pd
import numpy as np
import json

# %%
st.title("Dream Defenders Blocks File Transformer")

# %%
uploaded_file = st.file_uploader("Choose a file", type = 'csv')
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file, index_col=0)
    df['extras'] = df['extras'].apply(lambda x: json.loads(x))
    df_extras = df.extras.apply(pd.Series)
    df_clean = pd.merge(right=df, left=df_extras, right_index = True, left_index=True)
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')
    csv = convert_df(df_clean)
    st.download_button("Download Transformed File",
                        csv, 
                        'clean_blocks.csv', 
                        'text/csv', 
                        key='download-csv')


