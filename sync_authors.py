from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import csv
import glob
import gzip
import json
import os

SNAPSHOT_DIR = '/home/hadoopuser/openalex-snapshot'
CSV_DIR = '/home/hadoopuser/csv-files'

FILES_PER_ENTITY = int(os.environ.get('OPENALEX_DEMO_FILES_PER_ENTITY', '0'))

csv_files = {
    'authors': {
        'authors': {
            'name': os.path.join(CSV_DIR, 'authors_update.csv.gz'),
            'columns': [
                'id', 'orcid', 'display_name', 'display_name_alternatives', 'works_count', 'cited_by_count',
                'last_known_institution', 'works_api_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'authors_ids_update.csv.gz'),
            'columns': [
                'author_id', 'openalex', 'orcid', 'scopus', 'twitter', 'wikipedia', 'mag'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'authors_counts_by_year_update.csv.gz'),
            'columns': [
                'author_id', 'year', 'works_count', 'cited_by_count'
            ]
        }
    }
}


# https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
def _flatten_authors():
    file_spec = csv_files['authors']

    with gzip.open(file_spec['authors']['name'], 'wt', encoding='utf-8') as authors_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        authors_writer = csv.DictWriter(
            authors_csv, fieldnames=file_spec['authors']['columns'], extrasaction='ignore'
        )
        authors_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=file_spec['ids']['columns'])
        ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=file_spec['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')):
            string_id = jsonl_file_name.find("updated_date")
            then = pendulum.from_format(jsonl_file_name[string_id + 13:string_id + 23], 'YYYY-MM-DD', tz='local')
            now = pendulum.now()
            if now.diff(then).in_months() != 0:
                continue
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                for author_json in authors_jsonl:
                    if not author_json.strip():
                        continue

                    author = json.loads(author_json)

                    if not (author_id := author.get('id')):
                        continue

                    # authors
                    author['display_name_alternatives'] = json.dumps(author.get('display_name_alternatives'))
                    author['last_known_institution'] = (author.get('last_known_institution') or {}).get('id')
                    authors_writer.writerow(author)

                    # ids
                    if author_ids := author.get('ids'):
                        author_ids['author_id'] = author_id
                        ids_writer.writerow(author_ids)

                    # counts_by_year
                    if counts_by_year := author.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['author_id'] = author_id
                            counts_by_year_writer.writerow(count_by_year)
            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


with DAG(
        dag_id="sync_authors",
        schedule_interval="@monthly",
        catchup=False,
        start_date=pendulum.datetime(2022, 5, 1, tz="UTC")
) as dag:
    sync_aws = BashOperator(
        task_id="sync_aws",
        bash_command='aws s3 sync "s3://openalex/data/authors" "/home/hadoopuser/openalex-snapshot/data/authors" --no-sign-request',
    )

    filter_new_data = PythonOperator(
        task_id="filter_new_data",
        python_callable=_flatten_authors
    )

    unzip_new_data = BashOperator(
        task_id="unzip_new_data",
        bash_command='gunzip /home/hadoopuser/csv-files/authors*.gz'
    )

    move_to_hdfs = BashOperator(
        task_id="move_new_data_to_hdfs",
        bash_command='hdfs dfs -put -f /home/hadoopuser/csv-files/authors*.csv /csv_update/'
    )

    merge_authors = SparkSubmitOperator(
        task_id="merge_and_write_authors",
        application="hdfs://open1:9000/apps/spark_merge_authors.py",
        executor_cores=4,
        executor_memory="12G",
        num_executors=30,
        packages="co.datamechanics:delight_2.12:latest-SNAPSHOT",
        repositories="https://oss.sonatype.org/content/repositories/snapshots",
        conf={
            "spark.delight.accessToken.secret": "",
            "spark.extraListeners": "co.datamechanics.delight.DelightListener"
        }
    )

    swap_authors = BashOperator(
        task_id="swap_authors",
        bash_command='hdfs dfs -rm -r /csv_files/authors.csv '
                     '&& hdfs dfs -mv /csv_files/authors_updated.csv /csv_files/authors.csv'
    )

    swap_authors_counts = BashOperator(
        task_id="swap_authors_counts",
        bash_command='hdfs dfs -rm -r /csv_files/authors_counts_by_year.csv '
                     '&& hdfs dfs -mv /csv_files/authors_counts_by_year_updated.csv /csv_files/authors_counts_by_year.csv'
    )

    swap_authors_ids = BashOperator(
        task_id="swap_authors_ids",
        bash_command='hdfs dfs -rm -r /csv_files/authors_ids.csv '
                     '&& hdfs dfs -mv /csv_files/authors_ids_updated.csv /csv_files/authors_ids.csv'
    )

    clean_local = BashOperator(
        task_id="clean_local_files",
        bash_command='rm /home/hadoopuser/csv-files/*authors*'
    )

    sync_aws >> \
    filter_new_data >> \
    unzip_new_data >> \
    move_to_hdfs >> \
    merge_authors >> \
        [swap_authors,
         swap_authors_ids,
         swap_authors_counts] >> \
    clean_local
