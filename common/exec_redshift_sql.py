import redshift_connector
import boto3
import traceback
import logging
import sys
import json
import re
from awsglue.utils import getResolvedOptions
import base64
from botocore.exceptions import ClientError
import datetime
from dateutil.relativedelta import *
from copy import deepcopy
from mergedeep import merge


class ExecRedshiftSQL:
    def __init__(self, bucket, sql_file, redshift_secret_id, region_name, date_dict):
        redshift_connector.paramstyle = 'named'
        self.bucket = bucket
        self.sql_file = sql_file
        self.s3 = boto3.client('s3', region_name=region_name)
        self.redshift_secret_id = redshift_secret_id
        self.region_name = region_name
        self.date_dict = date_dict
        secret_dict = json.loads(self.get_secret())

        self.con = redshift_connector.connect(
            host=secret_dict["host"],
            database=secret_dict["database"],
            user=secret_dict["username"],
            password=secret_dict["password"]
        )

    def exec_single_sql(self, sql_str):
        logging.warning("run sql: {0}".format(sql_str))
        with self.con.cursor() as cursor:
            cursor.execute(sql_str)

    def exec(self):
        sqls = self.read_sql_file()
        sqls = self.remove_comments(sqls)
        sql_list = sqls.replace("\n", "").split(";")
        sql_list_trim = [sql.strip() for sql in sql_list if sql.strip() != ""]
        try:
            for sql in sql_list_trim:
                for key, value in self.date_dict.items():
                    sql = sql.replace(key, value)
                self.exec_single_sql(sql)
                self.con.commit()
        except Exception as e:
            # traceback.format_exc()
            logging.exception(e)
            self.con.close()
            raise e
        self.con.close()

    def remove_comments(self, sqls):
        out = re.sub(r'/\*.*?\*/', '', sqls, re.S)
        out = re.sub(r'--.*', '', out)
        return out

    def read_sql_file(self):
        data = self.s3.get_object(Bucket=self.bucket, Key=self.sql_file)
        contents = data['Body'].read().decode("utf-8")
        return contents

    def get_secret(self):
        secret_name = self.redshift_secret_id
        region_name = self.region_name

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return decoded_binary_secret


def parse_params_form_dict(source_dict):
    params = {}
    params_date_dict = {}
    for key, value in source_dict.items():
        if value is None or value == "":
            continue
        key = key.strip()
        value = value.strip()
        if "months" in value or "days" in value or "hours" in value or "%" in value:
            params_date_dict[key] = value
        else:
            params[key] = value
    params["params_data_dict"] = params_date_dict
    params["biz_date"] = format_biz_date()
    res_date = format_date_dict(params_date_dict, params["biz_date"])
    params["date_dict"] = res_date
    return params


def smart_params():
    glue_client = boto3.client("glue")
    source_param_list = sys.argv
    param_name_list = []
    for sp in source_param_list:
        if "--" in sp:
            param_name_list.append(sp.strip("--"))
    args = getResolvedOptions(sys.argv, param_name_list)
    if '--{}'.format('WORKFLOW_NAME') in sys.argv and '--{}'.format('WORKFLOW_RUN_ID') in sys.argv:
        workflow_name = args['WORKFLOW_NAME']
        workflow_run_id = args['WORKFLOW_RUN_ID']
        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                                                  RunId=workflow_run_id)["RunProperties"]

        workflow_params_res = parse_params_form_dict(workflow_params)
        job_params_res = parse_params_form_dict(args)

        res = merge(workflow_params_res, job_params_res)
        return res
    else:
        params = parse_params_form_dict(args)
    return params


def format_date_dict(date_dict, biz_date):
    # months=-1, days=-1, %Y-%m-%d
    # months=-1, days=-1, hours=-1, %Y-%m-%d %H
    # months=0, days=-2, hours=1, %Y-%m-%d %H
    # hours=-1,  %Y-%m-%d %H
    result = {}
    for key, value in date_dict.items():
        key = key.strip()
        value = value.strip()
        format_list = value.split(",")
        date_format_str = format_list[-1].strip()
        real_date_dict = {}
        for fl in format_list[0:-1]:
            flag = fl.split("=")[0].strip()
            num = fl.split("=")[1].strip()
            real_date_dict[flag] = int(num)
        if "months" not in real_date_dict:
            real_date_dict["months"] = 0
        if "days" not in real_date_dict:
            real_date_dict["days"] = 0
        if "hours" not in real_date_dict:
            real_date_dict["hours"] = 0
        if "minutes" not in real_date_dict:
            real_date_dict["minutes"] = 0
        real_date = biz_date + relativedelta(months=real_date_dict["months"], days=real_date_dict["days"],
                                             hours=real_date_dict["hours"], minutes=real_date_dict["minutes"])

        result[key] = real_date.strftime(date_format_str)
    return result


def format_biz_date():
    now = datetime.datetime.now()
    return now - datetime.timedelta(hours=now.hour, minutes=now.minute,
                                    seconds=now.second, microseconds=now.microsecond) + datetime.timedelta(days=-1)


# print((args["bucket"], args["sql_file"], args["redshift_secret_id"], args["region_name"])
# res = ExecRedshiftSQL("app-util", "test.sql", "redshift-producer", "ap-southeast-1").exec()
# print(format_exec_date("days=-1,%Y-%m-%d %H"))
# print(format_exec_date())
# print(format_exec_date("specify=123"))
# args = getResolvedOptions(sys.argv, ['bucket', 'sql_file', 'redshift_secret_id', 'region_name'])


args = smart_params()

logging.warning("job run params: " + str(args))
print("job run params: " + str(args))

ExecRedshiftSQL(args["bucket"], args["sql_file"], args["redshift_secret_id"], args["region_name"],
                args["date_dict"]).exec()
