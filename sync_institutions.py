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
    'institutions': {
        'institutions': {
            'name': os.path.join(CSV_DIR, 'institutions_update.csv.gz'),
            'columns': [
                'id', 'ror', 'display_name', 'country_code', 'type', 'homepage_url', 'image_url', 'image_thumbnail_url',
                'display_name_acroynyms', 'display_name_alternatives', 'works_count', 'cited_by_count', 'works_api_url',
                'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'institutions_ids_update.csv.gz'),
            'columns': [
                'institution_id', 'openalex', 'ror', 'grid', 'wikipedia', 'wikidata', 'mag'
            ]
        },
        'geo': {
            'name': os.path.join(CSV_DIR, 'institutions_geo_update.csv.gz'),
            'columns': [
                'institution_id', 'city', 'geonames_city_id', 'region', 'country_code', 'country', 'latitude',
                'longitude'
            ]
        },
        'associated_institutions': {
            'name': os.path.join(CSV_DIR, 'institutions_associated_institutions_update.csv.gz'),
            'columns': [
                'institution_id', 'associated_institution_id', 'relationship'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'institutions_counts_by_year_update.csv.gz'),
            'columns': [
                'institution_id', 'year', 'works_count', 'cited_by_count'
            ]
        }
    }
}


# https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
def _flatten_institutions():
    file_spec = csv_files['institutions']

    with gzip.open(file_spec['institutions']['name'], 'wt', encoding='utf-8') as institutions_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['geo']['name'], 'wt', encoding='utf-8') as geo_csv, \
            gzip.open(file_spec['associated_institutions']['name'], 'wt',
                      encoding='utf-8') as associated_institutions_csv, \
            gzip.open(file_spec['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        institutions_writer = csv.DictWriter(
            institutions_csv, fieldnames=file_spec['institutions']['columns'], extrasaction='ignore'
        )
        institutions_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=file_spec['ids']['columns'])
        ids_writer.writeheader()

        geo_writer = csv.DictWriter(geo_csv, fieldnames=file_spec['geo']['columns'])
        geo_writer.writeheader()

        associated_institutions_writer = csv.DictWriter(
            associated_institutions_csv, fieldnames=file_spec['associated_institutions']['columns']
        )
        associated_institutions_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=file_spec['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        seen_institution_ids = set()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'institutions', '*', '*.gz')):
            id = jsonl_file_name.find("updated_date")
            then = pendulum.from_format(jsonl_file_name[id + 13:id + 23], 'YYYY-MM-DD', tz='local')
            now = pendulum.now()
            if now.diff(then).in_months() != 0:
                continue
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as institutions_jsonl:
                for institution_json in institutions_jsonl:
                    if not institution_json.strip():
                        continue

                    institution = json.loads(institution_json)

                    if not (institution_id := institution.get('id')) or institution_id in seen_institution_ids:
                        continue

                    seen_institution_ids.add(institution_id)

                    # institutions
                    institution['display_name_acroynyms'] = json.dumps(institution.get('display_name_acroynyms'))
                    institution['display_name_alternatives'] = json.dumps(institution.get('display_name_alternatives'))
                    institutions_writer.writerow(institution)

                    # ids
                    if institution_ids := institution.get('ids'):
                        institution_ids['institution_id'] = institution_id
                        ids_writer.writerow(institution_ids)

                    # geo
                    if institution_geo := institution.get('geo'):
                        institution_geo['institution_id'] = institution_id
                        geo_writer.writerow(institution_geo)

                    # associated_institutions
                    if associated_institutions := institution.get(
                            'associated_institutions', institution.get('associated_insitutions')  # typo in api
                    ):
                        for associated_institution in associated_institutions:
                            if associated_institution_id := associated_institution.get('id'):
                                associated_institutions_writer.writerow({
                                    'institution_id': institution_id,
                                    'associated_institution_id': associated_institution_id,
                                    'relationship': associated_institution.get('relationship')
                                })

                    # counts_by_year
                    if counts_by_year := institution.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['institution_id'] = institution_id
                            counts_by_year_writer.writerow(count_by_year)

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


with DAG(
        dag_id="sync_institutions",
        schedule_interval="@monthly",
        catchup=False,
        start_date=pendulum.datetime(2022, 5, 1, tz="UTC")
) as dag:
    sync_aws = BashOperator(
        task_id="sync_aws",
        bash_command='aws s3 sync "s3://openalex/data/institutions" "/home/hadoopuser/openalex-snapshot/data/institutions" --no-sign-request',
    )

    filter_new_data = PythonOperator(
        task_id="filter_new_data",
        python_callable=_flatten_institutions
    )

    unzip_new_data = BashOperator(
        task_id="unzip_new_data",
        bash_command='gunzip /home/hadoopuser/csv-files/institutions*.gz'
    )

    move_to_hdfs = BashOperator(
        task_id="move_new_data_to_hdfs",
        bash_command='hdfs dfs -put -f /home/hadoopuser/csv-files/institutions*.csv /csv_update/'
    )

    merge_institutions = SparkSubmitOperator(
        task_id="merge_and_write_institutions",
        application="hdfs://open1:9000/apps/spark_merge_institutions.py",
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

    swap_institutions = BashOperator(
        task_id="swap_institutions",
        bash_command='hdfs dfs -rm -r /csv_files/institutions.csv '
                     '&& hdfs dfs -mv /csv_files/institutions_updated.csv /csv_files/institutions.csv'
    )

    swap_institutions_ids = BashOperator(
        task_id="swap_institutions_ids",
        bash_command='hdfs dfs -rm -r /csv_files/institutions_ids.csv '
                     '&& hdfs dfs -mv /csv_files/institutions_ids_updated.csv /csv_files/institutions_ids.csv'
    )

    swap_institutions_geo = BashOperator(
        task_id="swap_institutions_geo",
        bash_command='hdfs dfs -rm -r /csv_files/institutions_geo.csv '
                     '&& hdfs dfs -mv /csv_files/institutions_geo_updated.csv /csv_files/institutions_geo.csv'
    )

    swap_institutions_associated = BashOperator(
        task_id="swap_institutions_associated",
        bash_command='hdfs dfs -rm -r /csv_files/institutions_associated_institutions.csv '
                     '&& hdfs dfs -mv /csv_files/institutions_associated_institutions_updated.csv /csv_files/institutions_associated_institutions.csv'
    )

    swap_institutions_counts = BashOperator(
        task_id="swap_institutions_counts",
        bash_command='hdfs dfs -rm -r /csv_files/institutions_counts_by_year.csv '
                     '&& hdfs dfs -mv /csv_files/institutions_counts_by_year_updated.csv /csv_files/institutions_counts_by_year.csv'
    )

    clean_local = BashOperator(
        task_id="clean_local_files",
        bash_command='rm /home/hadoopuser/csv-files/*institutions*'
    )

    sync_aws >> \
    filter_new_data >> \
    unzip_new_data >> \
    move_to_hdfs >> \
    merge_institutions >> \
        [swap_institutions,
        swap_institutions_ids,
        swap_institutions_geo,
        swap_institutions_associated,
        swap_institutions_counts] >> \
    clean_local
