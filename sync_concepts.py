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
    'concepts': {
        'concepts': {
            'name': os.path.join(CSV_DIR, 'concepts_update.csv.gz'),
            'columns': [
                'id', 'wikidata', 'display_name', 'level', 'description', 'works_count', 'cited_by_count', 'image_url',
                'image_thumbnail_url', 'works_api_url', 'updated_date'
            ]
        },
        'ancestors': {
            'name': os.path.join(CSV_DIR, 'concepts_ancestors_update.csv.gz'),
            'columns': ['concept_id', 'ancestor_id']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'concepts_counts_by_year_update.csv.gz'),
            'columns': ['concept_id', 'year', 'works_count', 'cited_by_count']
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'concepts_ids_update.csv.gz'),
            'columns': ['concept_id', 'openalex', 'wikidata', 'wikipedia', 'umls_aui', 'umls_cui', 'mag']
        },
        'related_concepts': {
            'name': os.path.join(CSV_DIR, 'concepts_related_concepts_update.csv.gz'),
            'columns': ['concept_id', 'related_concept_id', 'score']
        }
    }
}


# https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
def _flatten_concepts():
    with gzip.open(csv_files['concepts']['concepts']['name'], 'wt', encoding='utf-8') as concepts_csv, \
            gzip.open(csv_files['concepts']['ancestors']['name'], 'wt', encoding='utf-8') as ancestors_csv, \
            gzip.open(csv_files['concepts']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv, \
            gzip.open(csv_files['concepts']['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(csv_files['concepts']['related_concepts']['name'], 'wt', encoding='utf-8') as related_concepts_csv:

        concepts_writer = csv.DictWriter(
            concepts_csv, fieldnames=csv_files['concepts']['concepts']['columns'], extrasaction='ignore'
        )
        concepts_writer.writeheader()

        ancestors_writer = csv.DictWriter(ancestors_csv, fieldnames=csv_files['concepts']['ancestors']['columns'])
        ancestors_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=csv_files['concepts']['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['concepts']['ids']['columns'])
        ids_writer.writeheader()

        related_concepts_writer = csv.DictWriter(related_concepts_csv, fieldnames=csv_files['concepts']['related_concepts']['columns'])
        related_concepts_writer.writeheader()

        seen_concept_ids = set()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'concepts', '*', '*.gz')):
            id = jsonl_file_name.find("updated_date")
            then = pendulum.from_format(jsonl_file_name[id+13:id+23], 'YYYY-MM-DD', tz='local')
            now = pendulum.now()
            if now.diff(then).in_months() != 0:
                continue
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as concepts_jsonl:
                for concept_json in concepts_jsonl:
                    if not concept_json.strip():
                        continue

                    concept = json.loads(concept_json)

                    if not (concept_id := concept.get('id')) or concept_id in seen_concept_ids:
                        continue

                    seen_concept_ids.add(concept_id)

                    concepts_writer.writerow(concept)

                    if concept_ids := concept.get('ids'):
                        concept_ids['concept_id'] = concept_id
                        concept_ids['umls_aui'] = json.dumps(concept_ids.get('umls_aui'))
                        concept_ids['umls_cui'] = json.dumps(concept_ids.get('umls_cui'))
                        ids_writer.writerow(concept_ids)

                    if ancestors := concept.get('ancestors'):
                        for ancestor in ancestors:
                            if ancestor_id := ancestor.get('id'):
                                ancestors_writer.writerow({
                                    'concept_id': concept_id,
                                    'ancestor_id': ancestor_id
                                })

                    if counts_by_year := concept.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['concept_id'] = concept_id
                            counts_by_year_writer.writerow(count_by_year)

                    if related_concepts := concept.get('related_concepts'):
                        for related_concept in related_concepts:
                            if related_concept_id := related_concept.get('id'):
                                related_concepts_writer.writerow({
                                    'concept_id': concept_id,
                                    'related_concept_id': related_concept_id,
                                    'score': related_concept.get('score')
                                })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


with DAG(
        dag_id="sync_concepts",
        schedule_interval="@monthly",
        catchup=False,
        start_date=pendulum.datetime(2022, 5, 1, tz="UTC")
) as dag:
    sync_aws = BashOperator(
        task_id="sync_aws",
        bash_command='aws s3 sync "s3://openalex/data/concepts" "/home/hadoopuser/openalex-snapshot/data/concepts" --no-sign-request',
    )

    filter_new_data = PythonOperator(
        task_id="filter_new_data",
        python_callable=_flatten_concepts
    )

    unzip_new_data = BashOperator(
        task_id="unzip_new_data",
        bash_command='gunzip /home/hadoopuser/csv-files/concepts*.gz'
    )

    move_to_hdfs = BashOperator(
        task_id="move_new_data_to_hdfs",
        bash_command='hdfs dfs -put -f /home/hadoopuser/csv-files/concepts*.csv /csv_update/'
    )

    merge_concepts = SparkSubmitOperator(
        task_id="merge_and_write_concepts",
        application="hdfs://open1:9000/apps/spark_merge_concepts.py",
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

    swap_concepts = BashOperator(
        task_id="swap_concepts",
        bash_command='hdfs dfs -rm -r /csv_files/concepts.csv '
                     '&& hdfs dfs -mv /csv_files/concepts_updated.csv /csv_files/concepts.csv'
    )

    swap_concepts_ancestors = BashOperator(
        task_id="swap_concepts_ancestors",
        bash_command='hdfs dfs -rm -r /csv_files/concepts_ancestors.csv '
                     '&& hdfs dfs -mv /csv_files/concepts_ancestors_updated.csv /csv_files/concepts_ancestors.csv'
    )

    swap_concepts_counts = BashOperator(
        task_id="swap_concepts_counts",
        bash_command='hdfs dfs -rm -r /csv_files/concepts_counts.csv '
                     '&& hdfs dfs -mv /csv_files/concepts_counts_updated.csv /csv_files/concepts_counts.csv'
    )
    
    swap_concepts_ids = BashOperator(
        task_id="swap_concepts_ids",
        bash_command='hdfs dfs -rm -r /csv_files/concepts_ids.csv '
                     '&& hdfs dfs -mv /csv_files/concepts_ids_updated.csv /csv_files/concepts_ids.csv'
    )
    
    swap_concepts_related = BashOperator(
        task_id="swap_concepts_related",
        bash_command='hdfs dfs -rm -r /csv_files/concepts_related.csv '
                     '&& hdfs dfs -mv /csv_files/concepts_related_updated.csv /csv_files/concepts_related.csv'
    )

    clean_local = BashOperator(
        task_id="clean_local_files",
        bash_command='rm /home/hadoopuser/csv-files/*concepts*'
    )

    sync_aws >> \
    filter_new_data >> \
    unzip_new_data >> \
    move_to_hdfs >> \
    merge_concepts >> \
        [swap_concepts,
         swap_concepts_ancestors,
         swap_concepts_counts,
         swap_concepts_ids,
         swap_concepts_related] >> \
    clean_local
