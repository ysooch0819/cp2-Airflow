from datetime import datetime, timedelta
import pendulum
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
# from airflow.models import Variable
# import os
# from dotenv import load_dotenv
# load_dotenv()



# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator


def extract_logs():
    """
    주어진 url에서 데이터를 추출하는 함수
    """
    import os
    import requests
    from cryptography.fernet import Fernet
    from dotenv import load_dotenv
    load_dotenv()
    
    print('==데이터 추출...')
    url = os.environ.get('log_url')
    logdata = requests.get(url).json()

    print('==복호화 키 생성 중...')
    key = bytes(os.environ.get('f_key'), encoding='utf-8')
    fernet = Fernet(key)
    print('==키 생성 완료==')
    # 암호화-복호화 및 데이터 변환하기
    decrypted_data = []
    print('==데이터 복호화 시작...')
    for log in logdata:
        target_data = fernet.decrypt(log['data'])
        decrypted_data.append(eval(target_data.decode('ascii')))
    print('==데이터 복호화 끝. 데이터 추출 완료==')
    
    return decrypted_data

def transform_logs(**context):
    """
    주어진 log를 문자열 변환하는 함수
    """
    logs_data = context['task_instance'].xcom_pull(task_ids='extract_logs')
    
    def b64_change_44(user_id):
        """
        user_id를 64자리 -> 44자리로 압축하는 함수
        """
        from b64uuid import B64UUID
        user_id_parts = [user_id[:32], user_id[32:]] # 32자리씩 나누기
        b64_uuid_parts = [B64UUID(u_id_part) for u_id_part in user_id_parts] # 32자리별로 B64UUID 모듈 활용하여 22자리로 변형
        b64_uuid_44 = str(b64_uuid_parts[0]) + str(b64_uuid_parts[1]) # 변형된 22자리 2개를 하나의 문자열로 합쳐준다.

        return b64_uuid_44

    def method_mapping(method):
        """
        method를 int로 맵핑하는 함수
        """
        method_mapping = {'GET': 1, 'POST': 2, 'PATCH': 3, 'PUT': 4, 'DELETE': 5}
        return method_mapping.get(method)

    def short_date(inDate):
        """
        inDate를 축약하는 함수
        """
        import re
        return re.sub(r'[^0-9]' ,'',''.join(inDate[2:]).replace(':','').replace('.',''))

    def compress_log(log):
        """
        log의 user_id, method, inDate를 압축하는 함수
        """
        tmp = log.copy()
        tmp['user_id'] = b64_change_44(tmp['user_id'])
        tmp['method'] = method_mapping(tmp['method'])
        tmp['inDate'] = short_date(tmp['inDate'])
        return tmp

    def transform_data(data):
        """
        추출한 data를 변환하는 함수
        """
        res = []
        print('==데이터 변환 중...')
        for log in data:
            res.append(compress_log(log))
        print('==데이터 변환 작업 완료==')
        return res

    # 데이터 변환 함수 호출
    transformed_data = transform_data(logs_data)
    # df = spark.createDataFrame(transformed_data)
    # df = df.coalesce(1)
    # context['task_instance'].xcom_push(key='transformed_data', value=df)
    return transformed_data

def load_logs(**context):
    """
    추출 후 변환된 데이터를 적재하는 함수
    AWS S3버킷에 '버킷이름/data/년/월/일/시' 폴더까지 구분하여 적재
    """
    # transform 에서 변환한 transformed_data 활용
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_logs')
    
    def get_s3_key(in_date, file_name):
        """
        주어진 in_date와 file_name 값으로 s3 객체의 key를 생성하는 함수
        """
        key_prefix = 'data'
        year = '20' + in_date[0:2]
        month = in_date[2:4]
        day = in_date[4:6]
        hour = in_date[6:8]

        return f'{key_prefix}/{year}/{month}/{day}/{hour}/{file_name}'

    def upload_to_s3(in_date, file_name, log_data):
        """
        주어진 in_date 값으로 s3 객체를 생성하고 log_data를 압축하여 file_name 으로 업로드하는 함수
        """
        import os
        import io
        import gzip
        import boto3
        from dotenv import load_dotenv
        load_dotenv()

        # 필요키 지정
        s3_aws_access_key_id = os.environ.get('s3_aws_access_key_id')
        s3_aws_secret_access_key = os.environ.get('s3_aws_secret_access_key')
        s3_bucket_name = os.environ.get('s3_bucket_name')

        s3_client = boto3.client('s3',
                    aws_access_key_id = s3_aws_access_key_id,
                    aws_secret_access_key=s3_aws_secret_access_key,
                    region_name='ap-northeast-2')

        bucket_path = f"s3://{s3_bucket_name}/{get_s3_key(in_date, file_name)}"
        
        # 압축알고리즘, gzip활용
        with io.BytesIO() as output:
            with gzip.GzipFile(fileobj=output, mode='w') as gzip_output:
                gzip_output.write(log_data.encode('ascii'))
            compressed_content = output.getvalue()

        # 압축된 파일을 s3에 업로드: 키 지정 함수 호출
        s3_client.upload_fileobj(
                io.BytesIO(compressed_content), 
                s3_bucket_name, 
                get_s3_key(in_date, f'{file_name}')
        )
        # 업로드 후 확인메시지 출력
        print(f"Upload completed: {file_name} to {bucket_path}")
        
    def load_data(logs_data):
        """
        주어진 logs_data를 시간 단위별로 분류하여 압축하여 s3에 업로드하는 함수
        """
        import json
        print('==데이터 적재 시작==')
        # 시간 단위별로 분류하기 위해 딕셔너리 초기화
        logs_by_hour = {}

        # 각 로그 데이터를 시간 단위로 분류
        for log_data in logs_data:
            in_date = log_data['inDate']
            hour = in_date[6:8]
            if hour not in logs_by_hour:
                logs_by_hour[hour] = []
            logs_by_hour[hour].append(log_data)
        
        # 분류한 시간 단위별로 로그 데이터를 압축하여 업로드 함수 호출
        for hour, logs in logs_by_hour.items():
            # 해당 시간의 첫번째 로그 데이터의 inDate 값을 가져옵니다.
            first_log_in_date = logs[0]['inDate']
            # 해당 시간의 마지막 로그 데이터의 inDate 값을 가져옵니다.
            last_log_in_date = logs[-1]['inDate']

            # 압축 파일의 이름: 첫로그 분단위 이하_끝로그 분단위 이하.gz 형태
            file_name = f"{first_log_in_date[8:]}_{last_log_in_date[8:]}.gz"
            
            # 시간 단위 폴더에 로그 데이터를 저장합니다.
            upload_to_s3(first_log_in_date[:8], file_name, json.dumps(logs))
        
        print('==데이터 적재 완료==')
    
    # 데이터 적재 함수 호출
    load_data(transformed_data)

default_args = {
    "retries": 2,
}

with DAG(
    "etl_pipeline",
    default_args=default_args,
    description="A simple ETL pipeline",
    schedule="*/3 * * * *",
    start_date=pendulum.datetime(2023, 4, 4, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"]
) as dag:
    # dag.doc_md = __doc__
    # SPARK_HOME = os.getenv("SPARK_HOME")
    # PYSPARK_PATH = f'{SPARK_HOME}/bin/spark-submit'
    # AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
    # AIRFLOW_DAG_PATH = f'{AIRFLOW_HOME}/dags'

    # # SparkSession 객체를 DAG에서 사용가능한 전역변수로 설정
    # conf = SparkConf().setAppName("test-spark").set("spark.executor.memory", "2g")
    # spark = SparkSession.builder \
    #     .master("local[1]") \
    #     .config(conf=conf).getOrCreate()

    # Extract Task
    extract = PythonOperator(
        task_id="extract_logs",
        python_callable=extract_logs,
        provide_context=True,
        dag=dag
    )

    # Transform Task
    transform = PythonOperator(
        task_id="transform_logs",
        python_callable=transform_logs,
        provide_context=True,
        dag=dag
    )

    # Load Task
    load = PythonOperator(
        task_id="load_logs",
        python_callable=load_logs,
        provide_context=True,
        dag=dag
    )

    # spark_start >> extract >> transform >> load
    extract >> transform >> load
