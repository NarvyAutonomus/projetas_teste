import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *

def init_spark():
    spark = SparkSession.builder.master("local[4]").appName('spark') \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")\
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")\
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")\
        .getOrCreate()

    return spark

def remove_accents(value):
    value = re.sub(r'[áÁàÀãÃ]', 'a', value)
    value = value.replace('ç', 'c')
    value = re.sub(r'[éÉèÈẽẼ]', 'e', value)
    value = re.sub(r'[íÍìÌ]', 'i', value)
    value = re.sub(r'[óÓòÒõÕ]', 'o', value)
    value = re.sub(r'[úÚùÙ]', 'u', value)

    return value

# Regex:
# group 1: (\s) - Agrupa espaço em primeiro grupo pra ser substituido por string vazia
# group 2: ([A-Z](?![A-Z]|\s|$)) - Caractér maíusculo que não precede outro caractér maísculo, espaço ou fim da string. Utilizado para demarcar separação das palavras em camelcase e ignorar iniciais como ICAO
def to_snake_case(value):
    return re.sub(r'(\s)|(?<=[a-zA-Z])([A-Z](?![A-Z]|\s|$))', r'_\2', value).lower()

@udf(ArrayType(StringType()))
def value_splitter(value, split = ' '):
    if value == None:
        return None

    separated_values = value.split(split)

    if len(separated_values) < 2:
        separated_values.append(None)

    return separated_values

