from airflow import DAG
from airflow.decorators import task
from core.pipeline import ELTPipeline
from projects.spacex.tasks import build_spacex_ingestion_pipeline

def create_spacex_dag(project_name, project_description, project_schedule):
    """ Função para criar a DAG do SpaceX """
    spacex_dag = ELTPipeline(
        name=project_name,
        description=project_description,
        schedule=project_schedule,
        ingestion_pipeline_func=build_spacex_ingestion_pipeline,
    ).create_dag()

    return spacex_dag

# Chamando a função para criar a DAG de SpaceX
spacex_dag = create_spacex_dag(
    project_name="spacex",
    project_description="🚀 SpaceX ELT pipeline: API -> Postgres -> DBT",
    project_schedule="@hourly"
)

print(f"DAG {spacex_dag.dag_id} created.")
