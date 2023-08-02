from snowflake.snowpark.functions import avg
import pandas as pd
import numpy as np
import json

## This is used to create a snowpark session
from snowflake.snowpark import Session

## Use this to create the UDF
from snowflake.snowpark.functions import udf, col
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.types import StringType

## These are necessary for the UDF function
import unidecode
import sys
import re
import os


def obterSessao(sessionDataFilePath="sessionData.json"):
    print("Starting session with",str(sessionDataFilePath))
    connection_parameters = json.loads(open(sessionDataFilePath,"r").read())
    session = Session.builder.configs(connection_parameters).create()
    return session