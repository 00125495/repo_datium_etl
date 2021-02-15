# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import numpy as np
import env as env
import os
import datetime
import json


# %%
import cape_privacy as cape
from cape_privacy.pandas import dtypes
from cape_privacy.pandas import transformations as tfms


# %%
class datiumETL:
    
    def __init__(self, file_name):
        """
        Object initialization for data management. It takes one argument, which is the file name.
        """
        self.file_name = file_name

    def load_data(self):
        """
        load csv file
        """
        extension = os.path.splitext(self.file_name)[1][1:]
        
        if extension == "csv":
            df=pd.read_csv(self.file_name)
            return df
        elif extension == "json":
            df=pd.read_json(self.file_name, lines=True)
            return df
        else:
            print("not valid extension...")
    
    def get_summary(self, df):
        """
        Option 1 : manual data exploration
        """
        types = df.dtypes
        counts = df.apply(lambda x: x.count())
        uniques = df.apply(lambda x: [x.unique()])
        nas = df.apply(lambda x: x.isnull().sum())
        distincts = df.apply(lambda x: x.unique().shape[0])
        missing = (df.isnull().sum() / df.shape[0]) * 100
        sk = df.skew()
        krt = df.kurt()
        
        print('Data shape:', df.shape)

        cols = ['Type', 'Total count', 'Null Values', 'Distinct Values', 'Missing Ratio', 'Unique Values', 'Skewness', 'Kurtosis']
        dtls = pd.concat([types, counts, nas, distincts, missing, uniques, sk, krt], axis=1, sort=False)
    
        dtls.columns = cols
        return dtls
        
    def get_missing_pct(self, df):

        """
        This function is to calculate the total missing value percentage

        """
        # get the number of missing data points per column
        missing_values_count = df.isnull().sum()

        # how many total missing values do we have?
        total_cells = np.product(df.shape)
        total_missing = missing_values_count.sum()

        # percent of data that is missing
        percent_missing = (total_missing/total_cells) * 100
        
        return "Total missing data percentage is %.4f" % (percent_missing)
    
    def get_date_fix(self, date_value):

        DATE_FORMATS = ['%A, %B %dth, %Y', '%B %dth, %Y', '%Y-%m-%dTEST', '%Y-%m-%d %H:%M:%S']

        for date_format in DATE_FORMATS:
            try:
                my_date = datetime.datetime.strptime(date_value, date_format)
                return my_date
            except ValueError:
                pass
            else:
                break
        else:
            my_date = '1900-01-01 00:00:00'
            return my_date
        
    def get_convert_sec_date(self, df, col_name):
        
        df[col_name]=pd.to_datetime(df[col_name], unit='s', errors='coerce')
        
        #return df

    def get_replace(self, df, column_name, original_values, new_values ):
        """ 
        To replace non numeric values with new values and convert to float
        column_name: column name for replace operation 
        original_values : list of original values
        new_values: list of new values
        """
        if df[column_name].isin(original_values).any():
            df[column_name].replace(original_values,new_values, inplace=True)
            df[column_name].fillna(np.nan)
            df[column_name] = df[column_name].astype('bool')
        return df[column_name]  
    
    def get_stand_decimal(self, df, column_name, decimal_points):
        
        df[column_name] = df[column_name].replace(['None', 'pyint'], [np.nan,np.nan]).fillna(0.00).astype('float64').round(decimals=2)
        
    def write_csv(self, df):
        
        compression_opts = dict(method='zip',archive_name=df.name+'.csv')
        df.to_csv(env.processed_data_dir+df.name+'.zip', index=False,compression=compression_opts, sep='|')
        print("Export Completed for "+ df.name)
        

        


# %%
data_csv = datiumETL(env.raw_data_file)
data_json = datiumETL(env.raw_data_file_json)


# %%
df=data_csv.load_data()


# %%
df_json=data_json.load_data()


# %%
df["created_at"]=df["created_at"].apply(data_csv.get_date_fix)


# %%
df["is_claimed"]= data_csv.get_replace(df,"is_claimed",['fal_se', 'truee']
                                                 , [False,True] )


# %%
data_csv.get_convert_sec_date(df, "last_login" )


# %%
data_csv.get_stand_decimal(df, "paid_amount", 2)


# %%
# policy based encryption 

policy = cape.parse_policy(env.policy_file)
caped_df = cape.apply_policy(policy, df)

caped_df.name="test"

data_csv.write_csv(caped_df)


# %%
user_details_temp=pd.DataFrame(df_json.user_details.values.tolist())

user_details=pd.DataFrame.from_records(user_details_temp)[['name','dob','address','username','password','national_id']]

user_details.name="user_details"

data_json.write_csv(user_details)


# %%
telephone_numbers=pd.DataFrame.from_records(user_details_temp)[['name', 'telephone_numbers']].fillna('No Status')
telephone_numbers["telephone_numbers"]=telephone_numbers["telephone_numbers"].apply(lambda x:','.join(map(str, x)))

telephone_numbers.name="telephone_numbers"

data_json.write_csv(telephone_numbers)


# %%
jobs_history_temp=pd.DataFrame(df_json.jobs_history.values.tolist())[0]

jobs_history=pd.DataFrame.from_records(jobs_history_temp)

jobs_history.name="jobs_history"

data_json.write_csv(jobs_history)


# %%



