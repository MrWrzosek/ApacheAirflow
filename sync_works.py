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
    'works': {
        'works': {
            'name': os.path.join(CSV_DIR, 'works_update.csv.gz'),
            'columns': [
                'id', 'doi', 'title', 'display_name', 'publication_year', 'publication_date', 'type', 'cited_by_count',
                'is_retracted', 'is_paratext', 'cited_by_api_url', 'abstract_inverted_index'
            ]
        },
        'host_venues': {
            'name': os.path.join(CSV_DIR, 'works_host_venues_update.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'alternate_host_venues': {
            'name': os.path.join(CSV_DIR, 'works_alternate_host_venues_update.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'authorships': {
            'name': os.path.join(CSV_DIR, 'works_authorships_update.csv.gz'),
            'columns': [
                'work_id', 'author_position', 'author_id', 'institution_id', 'raw_affiliation_string'
            ]
        },
        'biblio': {
            'name': os.path.join(CSV_DIR, 'works_biblio_update.csv.gz'),
            'columns': [
                'work_id', 'volume', 'issue', 'first_page', 'last_page'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'works_concepts_update.csv.gz'),
            'columns': [
                'work_id', 'concept_id', 'score'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'works_ids_update.csv.gz'),
            'columns': [
                'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
            ]
        },
        'mesh': {
            'name': os.path.join(CSV_DIR, 'works_mesh_update.csv.gz'),
            'columns': [
                'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui', 'qualifier_name', 'is_major_topic'
            ]
        },
        'open_access': {
            'name': os.path.join(CSV_DIR, 'works_open_access_update.csv.gz'),
            'columns': [
                'work_id', 'is_oa', 'oa_status', 'oa_url'
            ]
        },
        'referenced_works': {
            'name': os.path.join(CSV_DIR, 'works_referenced_works_update.csv.gz'),
            'columns': [
                'work_id', 'referenced_work_id'
            ]
        },
        'related_works': {
            'name': os.path.join(CSV_DIR, 'works_related_works_update.csv.gz'),
            'columns': [
                'work_id', 'related_work_id'
            ]
        },
    }
}


# https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
def _flatten_works():
    file_spec = csv_files['works']

    with gzip.open(file_spec['works']['name'], 'wt', encoding='utf-8') as works_csv, \
            gzip.open(file_spec['host_venues']['name'], 'wt', encoding='utf-8') as host_venues_csv, \
            gzip.open(file_spec['alternate_host_venues']['name'], 'wt', encoding='utf-8') as alternate_host_venues_csv, \
            gzip.open(file_spec['authorships']['name'], 'wt', encoding='utf-8') as authorships_csv, \
            gzip.open(file_spec['biblio']['name'], 'wt', encoding='utf-8') as biblio_csv, \
            gzip.open(file_spec['concepts']['name'], 'wt', encoding='utf-8') as concepts_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['mesh']['name'], 'wt', encoding='utf-8') as mesh_csv, \
            gzip.open(file_spec['open_access']['name'], 'wt', encoding='utf-8') as open_access_csv, \
            gzip.open(file_spec['referenced_works']['name'], 'wt', encoding='utf-8') as referenced_works_csv, \
            gzip.open(file_spec['related_works']['name'], 'wt', encoding='utf-8') as related_works_csv:

        works_writer = init_dict_writer(works_csv, file_spec['works'], extrasaction='ignore')
        host_venues_writer = init_dict_writer(host_venues_csv, file_spec['host_venues'])
        alternate_host_venues_writer = init_dict_writer(alternate_host_venues_csv, file_spec['alternate_host_venues'])
        authorships_writer = init_dict_writer(authorships_csv, file_spec['authorships'])
        biblio_writer = init_dict_writer(biblio_csv, file_spec['biblio'])
        concepts_writer = init_dict_writer(concepts_csv, file_spec['concepts'])
        ids_writer = init_dict_writer(ids_csv, file_spec['ids'], extrasaction='ignore')
        mesh_writer = init_dict_writer(mesh_csv, file_spec['mesh'])
        open_access_writer = init_dict_writer(open_access_csv, file_spec['open_access'])
        referenced_works_writer = init_dict_writer(referenced_works_csv, file_spec['referenced_works'])
        related_works_writer = init_dict_writer(related_works_csv, file_spec['related_works'])

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'works', '*', '*.gz')):
            id = jsonl_file_name.find("updated_date")
            then = pendulum.from_format(jsonl_file_name[id+13:id+23], 'YYYY-MM-DD', tz='local')
            now = pendulum.now()
            if now.diff(then).in_months() != 0:
                continue
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as works_jsonl:
                for work_json in works_jsonl:
                    if not work_json.strip():
                        continue

                    work = json.loads(work_json)

                    if not (work_id := work.get('id')):
                        continue

                    # works
                    if (abstract := work.get('abstract_inverted_index')) is not None:
                        work['abstract_inverted_index'] = json.dumps(abstract)

                    works_writer.writerow(work)

                    # host_venues
                    if host_venue := (work.get('host_venue') or {}):
                        if host_venue_id := host_venue.get('id'):
                            host_venues_writer.writerow({
                                'work_id': work_id,
                                'venue_id': host_venue_id,
                                'url': host_venue.get('url'),
                                'is_oa': host_venue.get('is_oa'),
                                'version': host_venue.get('version'),
                                'license': host_venue.get('license'),
                            })

                    # alternate_host_venues
                    if alternate_host_venues := work.get('alternate_host_venues'):
                        for alternate_host_venue in alternate_host_venues:
                            if venue_id := alternate_host_venue.get('id'):
                                alternate_host_venues_writer.writerow({
                                    'work_id': work_id,
                                    'venue_id': venue_id,
                                    'url': alternate_host_venue.get('url'),
                                    'is_oa': alternate_host_venue.get('is_oa'),
                                    'version': alternate_host_venue.get('version'),
                                    'license': alternate_host_venue.get('license'),
                                })

                    # authorships
                    if authorships := work.get('authorships'):
                        for authorship in authorships:
                            if author_id := authorship.get('author', {}).get('id'):
                                institutions = authorship.get('institutions')
                                institution_ids = [i.get('id') for i in institutions]
                                institution_ids = [i for i in institution_ids if i]
                                institution_ids = institution_ids or [None]

                                for institution_id in institution_ids:
                                    authorships_writer.writerow({
                                        'work_id': work_id,
                                        'author_position': authorship.get('author_position'),
                                        'author_id': author_id,
                                        'institution_id': institution_id,
                                        'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                                    })

                    # biblio
                    if biblio := work.get('biblio'):
                        biblio['work_id'] = work_id
                        biblio_writer.writerow(biblio)

                    # concepts
                    for concept in work.get('concepts'):
                        if concept_id := concept.get('id'):
                            concepts_writer.writerow({
                                'work_id': work_id,
                                'concept_id': concept_id,
                                'score': concept.get('score'),
                            })

                    # ids
                    if ids := work.get('ids'):
                        ids['work_id'] = work_id
                        ids_writer.writerow(ids)

                    # mesh
                    for mesh in work.get('mesh'):
                        mesh['work_id'] = work_id
                        mesh_writer.writerow(mesh)

                    # open_access
                    if open_access := work.get('open_access'):
                        open_access['work_id'] = work_id
                        open_access_writer.writerow(open_access)

                    # referenced_works
                    for referenced_work in work.get('referenced_works'):
                        if referenced_work:
                            referenced_works_writer.writerow({
                                'work_id': work_id,
                                'referenced_work_id': referenced_work
                            })

                    # related_works
                    for related_work in work.get('related_works'):
                        if related_work:
                            related_works_writer.writerow({
                                'work_id': work_id,
                                'related_work_id': related_work
                            })

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


def init_dict_writer(csv_file, file_spec, **kwargs):
    writer = csv.DictWriter(
        csv_file, fieldnames=file_spec['columns'], **kwargs
    )
    writer.writeheader()
    return writer


with DAG(
        dag_id="sync_works",
        schedule_interval="@monthly",
        catchup=False,
        start_date=pendulum.datetime(2022, 5, 1, tz="UTC")
) as dag:
    sync_aws = BashOperator(
        task_id="sync_aws",
        bash_command='aws s3 sync "s3://openalex/data/works" "/home/hadoopuser/openalex-snapshot/data/works" --no-sign-request',
    )

    filter_new_data = PythonOperator(
        task_id="filter_new_data",
        python_callable=_flatten_works
    )

    unzip_new_data = BashOperator(
        task_id="unzip_new_data",
        bash_command='gunzip /home/hadoopuser/csv-files/works*.gz'
    )

    move_to_hdfs = BashOperator(
        task_id="move_new_data_to_hdfs",
        bash_command='hdfs dfs -put -f /home/hadoopuser/csv-files/works*.csv /csv_update/'
    )

    merge_works = SparkSubmitOperator(
        task_id="merge_and_write_works",
        application="hdfs://open1:9000/apps/spark_merge_works.py",
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

    swap_works = BashOperator(
        task_id="swap_works",
        bash_command='hdfs dfs -rm -r /csv_files/works.csv '
                     '&& hdfs dfs -mv /csv_files/works_updated.csv /csv_files/works.csv'
    )

    swap_works_host_venues = BashOperator(
        task_id="swap_works_host_venues",
        bash_command='hdfs dfs -rm -r /csv_files/works_host_venues.csv '
                     '&& hdfs dfs -mv /csv_files/works_host_venues_updated.csv /csv_files/works_host_venues.csv'
    )
    
    swap_works_alternate_host_venues = BashOperator(
        task_id="swap_works_alternate_host_venues",
        bash_command='hdfs dfs -rm -r /csv_files/works_alternate_host_venues.csv '
                     '&& hdfs dfs -mv /csv_files/works_alternate_host_venues_updated.csv /csv_files/works_alternate_host_venues.csv'
    )
    
    swap_works_authorships = BashOperator(
        task_id="swap_works_authorships",
        bash_command='hdfs dfs -rm -r /csv_files/works_authorships.csv '
                     '&& hdfs dfs -mv /csv_files/works_authorships_updated.csv /csv_files/works_authorships.csv'
    )
    
    swap_works_biblio = BashOperator(
        task_id="swap_works_biblio",
        bash_command='hdfs dfs -rm -r /csv_files/works_biblio.csv '
                     '&& hdfs dfs -mv /csv_files/works_biblio_updated.csv /csv_files/works_biblio.csv'
    )
    
    swap_works_concepts = BashOperator(
        task_id="swap_works_concepts",
        bash_command='hdfs dfs -rm -r /csv_files/works_concepts.csv '
                     '&& hdfs dfs -mv /csv_files/works_concepts_updated.csv /csv_files/works_concepts.csv'
    )
    
    swap_works_ids = BashOperator(
        task_id="swap_works_ids",
        bash_command='hdfs dfs -rm -r /csv_files/works_ids.csv '
                     '&& hdfs dfs -mv /csv_files/works_ids_updated.csv /csv_files/works_ids.csv'
    )
    
    swap_works_mesh = BashOperator(
        task_id="swap_works_mesh",
        bash_command='hdfs dfs -rm -r /csv_files/works_mesh.csv '
                     '&& hdfs dfs -mv /csv_files/works_mesh_updated.csv /csv_files/works_mesh.csv'
    )
    
    swap_works_open_access = BashOperator(
        task_id="swap_works_open_access",
        bash_command='hdfs dfs -rm -r /csv_files/works_open_access.csv '
                     '&& hdfs dfs -mv /csv_files/works_open_access_updated.csv /csv_files/works_open_access.csv'
    )
    
    swap_works_referenced_works = BashOperator(
        task_id="swap_works_referenced_works",
        bash_command='hdfs dfs -rm -r /csv_files/works_referenced_works.csv '
                     '&& hdfs dfs -mv /csv_files/works_referenced_works_updated.csv /csv_files/works_referenced_works.csv'
    )
    
    swap_works_related_works = BashOperator(
        task_id="swap_works_related_works",
        bash_command='hdfs dfs -rm -r /csv_files/works_related_works.csv '
                     '&& hdfs dfs -mv /csv_files/works_related_works_updated.csv /csv_files/works_related_works.csv'
    )
    
    clean_local = BashOperator(
        task_id="clean_local_files",
        bash_command='rm /home/hadoopuser/csv-files/*works*'
    )

    sync_aws >> \
    filter_new_data >> \
    unzip_new_data >> \
    move_to_hdfs >> \
    merge_works >> \
        [swap_works,
         swap_works_host_venues,
         swap_works_alternate_host_venues,
         swap_works_authorships,
         swap_works_biblio,
         swap_works_concepts,
         swap_works_ids,
         swap_works_mesh,
         swap_works_open_access,
         swap_works_referenced_works,
         swap_works_related_works] >> \
    clean_local
