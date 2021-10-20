from statsmodels.tsa.holtwinters import ExponentialSmoothing
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import sys


def connection():
    engine = create_engine('mysql+pymysql://docker_user:1234@192.168.56.1/mlops_model')
    return engine


def model_train(train_id):
    df = pd.read_sql(f'select series_id, period, value from data  where train_id = {train_id} '
                     f'order by series_id, period', connection())
    print(f'Train ID : {train_id} started\n')
    conn_obj = connection().connect()
    conn_obj.execute(f"update train_state set state='P' where train_id = {train_id}")
    conn_obj.close()

    try:

        for i in np.unique(np.array(df['series_id'])):
            sub_df = df[df['series_id'] == i].copy()
            ets = ExponentialSmoothing(endog=sub_df['value'].map(float))
            model = ets.fit()
            sub_df['value'] = list(model.fittedvalues)
            sub_df['status'] = 'S'
            sub_df['train_id'] = train_id
            sub_df.to_sql(name='data', con=connection(), if_exists='append', index=False)

        conn_obj = connection().connect()
        conn_obj.execute(f"update train_state set state='S' where train_id = {train_id}")
        conn_obj.close()
        print(f'Train ID : {train_id} successful\n')

    except Exception as e:
        conn_obj = connection().connect()
        conn_obj.execute(f"update train_state set state='F' where train_id = {train_id}")
        conn_obj.close()
        print(f'Train ID : {train_id} failed\n')
        print(f'Error : {e}')


if __name__ == '__main__':
    model_train(sys.argv[1])
