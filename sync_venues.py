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
    'venues': {
        'venues': {
            'name': os.path.join(CSV_DIR, 'venues_update.csv.gz'),
            'columns': [
                'id', 'issn_l', 'issn', 'display_name', 'publisher', 'works_count', 'cited_by_count', 'is_oa',
                'is_in_doaj', 'homepage_url', 'works_api_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'venues_ids_update.csv.gz'),
            'columns': ['venue_id', 'openalex', 'issn_l', 'issn', 'mag']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'venues_counts_by_year_update.csv.gz'),
            'columns': ['venue_id', 'year', 'works_count', 'cited_by_count']
        },
    }
}


# https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
def _flatten_venues():
    with gzip.open(csv_files['venues']['venues']['name'], 'wt', encoding='utf-8') as venues_csv, \
            gzip.open(csv_files['venues']['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(csv_files['venues']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        venues_writer = csv.DictWriter(
            venues_csv, fieldnames=csv_files['venues']['venues']['columns'], extrasaction='ignore'
        )
        venues_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['venues']['ids']['columns'])
        ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=csv_files['venues']['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        seen_venue_ids = set()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'venues', '*', '*.gz')):
            id = jsonl_file_name.find("updated_date")
            then = pendulum.from_format(jsonl_file_name[id+13:id+23], 'YYYY-MM-DD', tz='local')
            now = pendulum.now()
            if now.diff(then).in_months() != 0:
                continue
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as venues_jsonl:
                for venue_json in venues_jsonl:
                    if not venue_json.strip():
                        continue

                    venue = json.loads(venue_json)

                    if not (venue_id := venue.get('id')) or venue_id in seen_venue_ids:
                        continue

                    seen_venue_ids.add(venue_id)

                    venue['issn'] = json.dumps(venue.get('issn'))
                    venues_writer.writerow(venue)

                    if venue_ids := venue.get('ids'):
                        venue_ids['venue_id'] = venue_id
                        venue_ids['issn'] = json.dumps(venue_ids.get('issn'))
                        ids_writer.writerow(venue_ids)

                    if counts_by_year := venue.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['venue_id'] = venue_id
                            counts_by_year_writer.writerow(count_by_year)

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


with DAG(
        dag_id="sync_venues",
        schedule_interval="@monthly",
        catchup=False,
        start_date=pendulum.datetime(2022, 5, 1, tz="UTC")
) as dag:
    sync_aws = BashOperator(
        task_id="sync_aws",
        bash_command='aws s3 sync "s3://openalex/data/venues" "/home/hadoopuser/openalex-snapshot/data/venues" --no-sign-request',
    )

    filter_new_data = PythonOperator(
        task_id="filter_new_data",
        python_callable=_flatten_venues
    )

    unzip_new_data = BashOperator(
        task_id="unzip_new_data",
        bash_command='gunzip /home/hadoopuser/csv-files/venues*.gz'
    )

    move_to_hdfs = BashOperator(
        task_id="move_new_data_to_hdfs",
        bash_command='hdfs dfs -put -f /home/hadoopuser/csv-files/venues*.csv /csv_update/'
    )

    merge_venues = SparkSubmitOperator(
        task_id="merge_and_write_venues",
        application="hdfs://open1:9000/apps/spark_merge_venues.py",
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

    swap_venues = BashOperator(
        task_id="swap_venues",
        bash_command='hdfs dfs -rm -r /csv_files/venues.csv '
                     '&& hdfs dfs -mv /csv_files/venues_updated.csv /csv_files/venues.csv'
    )
    
    swap_venues_ids = BashOperator(
        task_id="swap_venues_ids",
        bash_command='hdfs dfs -rm -r /csv_files/venues_ids.csv '
                     '&& hdfs dfs -mv /csv_files/venues_ids_updated.csv /csv_files/venues_ids.csv'
    )

    swap_venues_counts = BashOperator(
        task_id="swap_venues_counts",
        bash_command='hdfs dfs -rm -r /csv_files/venues_counts.csv '
                     '&& hdfs dfs -mv /csv_files/venues_counts_updated.csv /csv_files/venues_counts.csv'
    )

    clean_local = BashOperator(
        task_id="clean_local_files",
        bash_command='rm /home/hadoopuser/csv-files/*venues*'
    )

    sync_aws >> \
    filter_new_data >> \
    unzip_new_data >> \
    move_to_hdfs >> \
    merge_venues >> \
        [swap_venues,
         swap_venues_ids,
         swap_venues_counts] >> \
    clean_local
