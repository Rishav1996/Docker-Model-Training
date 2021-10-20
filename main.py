import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import os


def connection():
    engine = create_engine('mysql+pymysql://root:1234@localhost/mlops_model')
    return engine


st.set_page_config(page_title='MLOPs', layout='wide')
'# Docker Model Training'

file = st.file_uploader('Upload File', accept_multiple_files=False, type='.csv')

if file:
    if st.button('Submit file'):
        df = pd.read_csv(file)
        df['status'] = 'I'
        train_id = pd.read_sql('select distinct train_id from data order by train_id desc', connection()).values
        if len(train_id) == 0:
            train_id = 1
        else:
            train_id = train_id[0]
            train_id = int(train_id[-1]) + 1
        df['train_id'] = train_id
        df.to_csv(f'E:/Workspace/Web Analysis/Docker Model Training/data/data_{train_id}.csv', index=False)
        df.to_sql(name='data', con=connection(), if_exists='append', index=False)

        state = pd.read_sql(f'select distinct state from train_state where train_id = {train_id}', connection()).values
        df = pd.DataFrame([[train_id, 'I']], columns=['train_id', 'state'])
        df.to_sql(name='train_state', con=connection(), if_exists='append', index=False)
        f'Train ID Generated : {train_id}'

train_ids = np.array(pd.read_sql(f"select distinct train_id from train_state where state = 'I'",
                                 connection()).values).flatten()

if len(train_ids) > 0:
    option_selected = st.selectbox(label='Train IDs', options=train_ids)
    if st.button('Train Model'):
        f"{os.popen(f'docker run --name train_image_{option_selected} train_image {option_selected}').read()}"
        f"Container removed {os.popen(f'docker rm train_image_{option_selected}').read()}"


successful_train_ids = np.array(pd.read_sql(f"select distinct train_id from train_state where state = 'S'",
                                            connection()).values).flatten()

if len(successful_train_ids) > 0:
    option_selected = st.selectbox(label='Train IDs', options=successful_train_ids)
    true_values = pd.read_sql(f"select period, value from data where status = 'I' and train_id = {option_selected} order by period",
                              connection())
    true_values.index = true_values['period']
    true_values.drop(columns=['period'], inplace=True)
    true_values.rename(columns={'value': 'true_values'}, inplace=True)
    true_values['true_values'] = true_values['true_values'].map(float)

    pred_values = pd.read_sql(f"select period, value from data where status = 'S' and train_id = {option_selected} order by period",
                              connection())
    pred_values.index = pred_values['period']
    pred_values.drop(columns=['period'], inplace=True)
    pred_values.rename(columns={'value': 'pred_values'}, inplace=True)
    pred_values['pred_values'] = pred_values['pred_values'].map(float)
    st.line_chart(pd.merge(left=true_values, right=pred_values, right_index=True, left_index=True).tail(12))
