from typing import Optional
import chardet
import numpy as np
import pandas as pd
import decimal
import re

input_csv_filename = 'data/donors.csv'
output_parquet_filename = 'data/donors.parquet'

def trans_generic(obj: object) -> str:
    val = str(obj).strip()
    val = re.sub('\n', '', val)
    return val

def trans_donor_id(obj: object) -> str:
    donor_id = trans_generic(obj)
    try:
        dec = decimal.Decimal(donor_id)
        return str(abs(int(dec)))
    except Exception as _:
        return donor_id

def trans_postcode(obj: object) -> str:
    postcode = trans_generic(obj)
    postcode = re.sub('^\d', '', postcode)
    return postcode

def trans_gender(obj: object) -> Optional[str]:
    gender = trans_generic(obj).upper()
    gender = re.sub('[^F|^M]', '', gender)
    return None if gender == '' else gender

def trans_donor_type(obj: object) -> str:
    donor_type = trans_generic(obj)
    donor_type = re.sub('[^\d|^.]', '', donor_type)
    return '0' if donor_type in ['', '.'] else donor_type

with open(input_csv_filename, 'rb') as f:
    rawdata = b''.join([f.readline() for _ in range(10)])
    encoding = chardet.detect(rawdata)['encoding']
    df = pd.read_csv(input_csv_filename, encoding=encoding, quotechar='"')
    df['donor_id'] = df['donor_id'].apply(
        lambda obj : trans_donor_id(obj)
    )
    df['postcode'] = df['postcode'].apply(
        lambda obj : trans_postcode(obj)
    )
    df['gender'] = df['gender'].apply(
        lambda obj : trans_gender(obj)
    )
    df['birth_date'] = df['birth_date'].apply(
        lambda obj: trans_generic(obj)
    )
    pd.to_datetime(df['birth_date'], errors='coerce').astype('datetime64[ns]')
    df['donor_type'] = df['donor_type'].apply(
        lambda obj: trans_donor_type(obj)
    )
    pd.to_numeric(df['donor_type'], errors='coerce').astype('int32')

    df.to_parquet(output_parquet_filename)

    pdf = pd.read_parquet(output_parquet_filename)
    print(pdf.info())