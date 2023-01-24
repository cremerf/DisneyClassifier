import pandas as pd
from datetime import date
import requests
from botocore.exceptions import NoCredentialsError, ClientError
import boto3
import sys

#NOTE: EL ARCHIVO ORIGINAL Q TENÍA LAS FUNCIONES DE AWS SE LLAMA "bajadaDatos.ipynb"

class GlobantAWS():
    def __init__(self) -> None:
        self.full_local_input_path = '/tmp/'
        self.filename_cat_subcat = 'df_cat_subcat.xlsx'
        self.s3_bucket_name = 'dt-revenue-research-dev'
        self.prefix_s3_input = 'mercado_libre/input/datos/'
        self.prefix_s3_output = 'mercado_libre/output/trend/'
        self.df_to_append_filename = "temp_df_subcat.csv"
        self.df_result_filename = 'df_subcat.csv'
        
    def download_from_aws_s3(self, bucket, s3_file, local_file_full_path):
        s3 = boto3.client('s3')
        try:
            s3.download_file(bucket, s3_file, local_file_full_path)
            return True
        except NoCredentialsError:
            print("Credentials not available")
            return False
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
                raise
            else:
                raise
        except:
            print("Unexpected error:" + str(sys.exc_info()[0]))
            raise


    def append_local_put_into_s3(self, in_bucket, df_in_s3_filename, df_to_append_filename, prefix_s3):
        results = False
        df_append = pd.read_csv(self.full_local_input_path + df_to_append_filename, header=None, sep=';', encoding='utf-8 sig')
        try:
            s3 = boto3.resource('s3')
            try:
                s3.Object(in_bucket, prefix_s3).load()
                self.download_from_aws_s3(self.s3_bucket_name, prefix_s3, self.full_local_input_path + df_in_s3_filename)

                df_s3 = pd.read_csv(self.full_local_input_path + df_in_s3_filename, header=None, sep=';', encoding='utf-8 sig')

                df_s3 = df_s3.append(df_append)

            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    df_s3 = df_append

            out_bucket = s3.Bucket(in_bucket)
            obj = out_bucket.Object(prefix_s3)
            obj.put(Body=df_s3.to_csv(sep=';', encoding='utf-8 sig', index=False, header=None))
            results = True
        except Exception as error:
            print("Error on putting dataframe in s3 " + str(error))
            raise
        return results