import io
import sys
import boto3
import fiona
import time
import os
import shapely
import geopandas as gpd
import pandas as pd
from uuid import uuid4
from pyproj import Geod
from shapely import wkt

bucket = 'ffdp-data-general-stage'
folder_path = "geospatial/field_boundary_detection/kml/" 


def get_aws_creds():
    cred = boto3.Session().get_credentials()
    aws_access_key_id=cred.access_key
    aws_secret_access_key=cred.secret_key
    aws_session_token=cred.token
    return aws_access_key_id, aws_secret_access_key, aws_session_token

def initiate_s3_connection():
    aws_access_key_id, aws_secret_access_key, aws_session_token = get_aws_creds()
    client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    aws_session_token = aws_session_token)
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('ffdp-data-general-stage')
    return my_bucket,client

def read_file_to_fr(obj,file_key):        #read the original files and return the geopnadas dataframe
    gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
    if file_key.endswith('.kml'):
        fr = gpd.read_file(io.BytesIO(obj['Body'].read()),driver='KML')
    elif file_key.endswith('.geojson'):
        fr = gpd.read_file(io.BytesIO(obj['Body'].read()))
    if not fr.empty:
        fr = fr.dropna(subset=['geometry'])
        fr = fr[['geometry']]
    fr['geometry'] = fr['geometry'].apply(lambda x: x if not x.has_z else shapely.wkb.loads(shapely.wkb.dumps(x, output_dimension=2)))
    fr = fr.loc[fr.geometry.geometry.type != 'Point']
#         fr['file_name'] = file_key.split('/')[-1]
    return fr

def calculate_net_area(res_union):
    net_area = res_union.to_crs('+proj=cea').area.sum()/10000
    print(f'net area under management is {net_area} hectares')  


def get_net_area_under_management():
    tic = time.time()
    bucket_connection,client = initiate_s3_connection()
    fin_fr = pd.DataFrame()
    file_count = 0
    for file in bucket_connection.objects.all():
        file_key = file.key
        if folder_path in file_key:
            obj = client.get_object(Bucket=bucket, Key=file_key)
            ind_fr = read_file_to_fr(obj,file_key)
            if not ind_fr.empty:
    #             print(ind_fr.head())
                if file_count == 0:
                    file_count = file_count+1
                    empty_gpd = gpd.GeoDataFrame(columns=['geometry'], geometry='geometry',crs='WGS84')
                    res_union = gpd.overlay(ind_fr,empty_gpd, how='union')
    #                 print(res_union.head())
                    continue
                else:
                    file_count = file_count+1
                    res_union = gpd.overlay(res_union,ind_fr, how='union')
            else:
                file_count = file_count+1
                print(f"file name: {file_key.split('/')[-1]} is empty")
    calculate_net_area(res_union)
    toc = time.time()
    print(f'total time taken {(toc-tic)/60} minutes')
    print(f'Total files read: {file_count}')


if __name__ == '___main__':
    get_net_area_under_management()


