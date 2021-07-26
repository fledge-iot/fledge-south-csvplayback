import pandas as pd
import os
import argparse
import sys
import numpy as np

_FLEDGE_ROOT = os.getenv("FLEDGE_ROOT", default='/usr/local/fledge')
if not os.path.exists(_FLEDGE_ROOT):
    print('Make sure FOGLAMP_ROOT exist')
    sys.exit(1)

_FLEDGE_DATA = os.path.expanduser(_FLEDGE_ROOT + '/data')

"""
Usage 
python3 process_csv_data.py --input_file_name vibe-2019-12-12.csv --output_file_name vibration.csv --chunksize 10000
or 
python3 process_csv_data.py --input_file_name vibe-2019-12-12.csv --output_file_name vibration.csv --chunksize 10000 --choice fill --method linear


"""

ap = argparse.ArgumentParser()
ap.add_argument("-i", "--input_file_name", required=True,
                help="Name of input_file")
ap.add_argument("-o", "--output_file_name", required=True,
                help="File name of processed file")
ap.add_argument("-c", "--chunksize", type=int, default=10000,
                help="Chunk size")
ap.add_argument("-C", "--choice", type=str, default='ignore',
                help="Fill or drop or ignore NaN values")
ap.add_argument("-m", "--method", type=str, default='linear',
                help="method for filling data")

args = vars(ap.parse_args())

out_file_name = args['output_file_name']
in_file_name = args['input_file_name']
csv_out_path = '{}/{}'.format(_FLEDGE_DATA, out_file_name)
csv_in_path = '{}/{}'.format(_FLEDGE_DATA, in_file_name)

if not os.path.exists(csv_in_path):
    print('The input file does not exist')
    sys.exit(1)

if os.path.exists(csv_out_path):
    print('The converted file already exists change the output file name or delete the earlier converted file.')
    sys.exit(1)

chunksize = args['chunksize']
choice = args['choice']
method = args['method']


def get_clean_csv_file(csv_in_path, csv_out_path, chunksize=10000):
    """
    Converts a raw csv file of format like "{""channel1"":0.0083912037,""channel2"":0.0071383551}"
    to a format like    channel1,    channel2
                        0.0083912037,0.0071383551
    Args:
        csv_in_path: Name of input_file (Full path)
        csv_out_path: Full File path of processed file (will be stored in same directory as input file)
        chunksize: The chunk size required to process the csv file

    Returns: None

    """
    df_iter = pd.read_csv(csv_in_path, chunksize=chunksize, iterator=True)
    i = 0
    while True:
        try:
            if i == 0:
                df = next(df_iter)
                df['channel1'] = df['reading'].apply(lambda x: x.split(",")[0].split(":")[1])
                df['channel2'] = df['reading'].apply(lambda x: x.split(",")[1].split(":")[1].split("}")[0])
                df[['channel1', 'channel2', 'user_ts']].to_csv(csv_out_path, mode='a', index=False)
            else:
                df = next(df_iter)
                df['channel1'] = df['reading'].apply(lambda x: x.split(",")[0].split(":")[1])
                df['channel2'] = df['reading'].apply(lambda x: x.split(",")[1].split(":")[1].split("}")[0])
                df[['channel1', 'channel2', 'user_ts']].to_csv(csv_out_path, mode='a', index=False, header=None)
            i += 1
        except StopIteration:
            break


def remove_nan_from_csv(csv_in_path, csv_out_path, chunksize=8000, choice='fill', method='linear'):
    """
    Removes NaN from the csv file and stores the modified file in specified location.
    Args:
        csv_in_path: Name of input_file (Full path)
        csv_out_path: Full File path of processed file (will be stored in same directory as input file)
        chunksize: The chunk size required to process the csv file
        choice: Whether to fill or drop the data.
        method: interpolation methods like linear, cubic, nearest and sliding window methods like
           rolling mean and rolling median.

    Returns: None

    """
    df_iter = pd.read_csv(csv_in_path, chunksize=chunksize, iterator=True)
    i = 0
    while True:
        try:

            df = next(df_iter)
            original_column_list = df.columns
            for col_name in df.columns:
                is_nan = df[col_name].isnull().values.any()  # check if NaN values

                is_blank = df[col_name].astype(str).str.isspace().any()  # check if blank values are there

                all_values_null_or_blank = df[col_name].isnull().values.all() or \
                                           df[col_name].astype(str).str.isspace().all()
                if is_nan or is_blank:
                    if not all_values_null_or_blank:
                        if is_blank:
                            # convert white spaces to NaN's
                            df[col_name].replace(r'^\s*$', np.nan, regex=True, inplace=True)
                        if choice == 'fill':
                            df[col_name] = df[col_name].astype('float64')
                            fill_method = method

                            if fill_method == 'linear' or fill_method == 'cubic' or fill_method == 'nearest':
                                df[col_name].interpolate(method=fill_method, inplace=True,
                                                         limit_direction='both')

                            elif fill_method == 'rolling_mean':
                                df[col_name].fillna(df[col_name].rolling(2, min_periods=1).mean(),
                                                    inplace=True)

                            elif fill_method == 'rolling_median':
                                df[col_name].fillna(df[col_name].rolling(2, min_periods=1).median(),
                                                    inplace=True)
                        else:
                            original_column_list.remove(col_name)
                    else:
                        # Drop everything in this column
                        original_column_list.remove(col_name)

            if i == 0:
                df[original_column_list].to_csv(csv_out_path, mode='a', index=False)
            else:
                df[original_column_list].to_csv(csv_out_path, mode='a', index=False, header=None)

            i += 1
        except StopIteration:
            break


get_clean_csv_file(csv_in_path, csv_out_path, chunksize)
if method != 'ignore':
    remove_nan_from_csv(csv_in_path, csv_out_path, chunksize, choice, method)
