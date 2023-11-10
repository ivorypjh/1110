import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from urllib import request
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# DAG 생성
# 동일한 파일에 덮어쓰기를 하므로 작업은 한 번에 하나씩 수행
# sql 구문이 들어있는 파일의 경로를 추가 - template_searchpath
dag = DAG(
    dag_id="s3_upload",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath = "/tmp",
    max_active_runs=1 # 일을 하나씩 처리하도록 설정
)

# 매개변수 사용
# 파이썬 함수가 dict 를 매개변수로 받으면 실행 시 만들어지는 
# 모든 매개변수를 넘겨 받아서 사용할 수 있음
def _get_data(year, month, day, hour, output_path, **_):
    # 시간은 2시간 전으로 설정
    hour = str(int(hour) - 2)
    if int(hour) < 0:
        hour = str(int(hour) + 24)
        day = str(int(day) - 1)

    # 다운로드 받을 URL 생성
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    print(url)
    # 데이터 다운 받기
    request.urlretrieve(url, output_path)

# 데이터를 넘겨줘서 위임하는 파이썬 오퍼레이터
# 데이터 다운로드를 수행
get_data = PythonOperator(
    task_id = "get_data",
    python_callable = _get_data,
    # 매개변수 전달을 위해 op_kwargs 사용
    op_kwargs = {
        "year" : "{{execution_date.year}}",
        "month" : "{{execution_date.month}}",
        "day" : "{{execution_date.day}}",
        "hour" : "{{execution_date.hour}}",
        "output_path" : "/tmp/wikipageview.gz",
    },
    dag=dag,
)

# 데이터 압축을 해제하는 bash 오퍼레이터
extrach_gz = BashOperator(
    task_id = "extract_gz",
    # command 명령을 사용
    # 파일 경로는 위의 오퍼레이터와 동일하게 설정
    bash_command = "gunzip --force /tmp/wikipageview.gz",
    dag=dag
)

# 원하는 데이터 추출을 처리하는 함수
def _fetch_pageviews(pagenames, execution_date, **_):
    # 처음엔 기본 값 0으로 설정
    result = dict.fromkeys(pagenames, 0)
    # 파일을 열어서 가져오기
    with open("/tmp/wikipageview", "r") as f:
        # pagenames 에 해당하는 사이트의 페이지 뷰만 dict 로 생성해서 저장
        for line in f:
            # 공백을 기준으로 구분해서 변수로 받아옴
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    print(result)

    # pageview 테이블에 데이터를 삽입하는 sql 을 파일에 기록
    with open("/tmp/postgres_query.sql", "w") as f:
        # items 를 사용해서 데이터르 전부 가져옴
        for pagename, pageviewcount in result.items():
            f.write("INSERT INTO pageview_counts VALUES ("
                    # pageviewcount 는 숫자이므로 따옴표(') 를 사용하지 않음
                    f"'{pagename}', {pageviewcount}, '{execution_date}'"
                    ");\n")


# 원하는 데이터만 추출해서 출력하는 오퍼레이터
fetch_pageviews = PythonOperator(
    task_id = "fetch_pageviews",
    python_callable = _fetch_pageviews,
    # 함수에 넘겨줄 매개변수
    op_kwargs = {"pagenames" : {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag
)

# postgresql 에 데이터를 저장하는 Operator 를 생성
write_to_postgres = PostgresOperator(
    task_id = "write_to_postgre",
    postgres_conn_id = "my_postgres",
    # 여기의 파일 이름은 위에서 작성한 sql 파일 이름과 일치해야 함
    sql = "postgres_query.sql", 
    dag = dag
)

# S3 에 파일을 전송하는 함수
def upload_to_s3(filename:str, key:str, bucket_name:str) -> None:
    hook = S3Hook('aws-default')
    hook.load_file(filename = filename, key = key, bucket_name = bucket_name)

upload = PythonOperator(
    task_id = "upload",
    python_callable = upload_to_s3,
    op_kwargs = {
        'filename': "/tmp/wikipageviews",
        'key' : "wikipageviews",
        "bucket_name" : "mybucket" # 자신의 버킷 이름
    }
)

# 순서 설정
get_data >> extrach_gz >> fetch_pageviews >> write_to_postgres >> upload
