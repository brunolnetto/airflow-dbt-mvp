from dataclasses import dataclass
from airflow import DAG
from typing import Callable

from core.config import DBT_DEFAULT_ARGS, get_target_env
from core.tasks import get_transform_pipeline_group

@dataclass
class ELTPipeline:
    name: str
    description: str
    schedule: str
    ingestion_pipeline_func: Callable

    def create_dag(self) -> DAG:
        target_env = get_target_env()
        dag_id = f"{self.name}__elt__{target_env}"

        with DAG(
            dag_id=dag_id,
            default_args=DBT_DEFAULT_ARGS,
            description=self.description,
            schedule_interval=self.schedule,
            catchup=False,
            tags=[self.name, "elt", "dbt"],
        ) as dag:

            ingestion_pipeline = self.ingestion_pipeline_func(
                pipeline_name=f"ingestion_pipeline_{self.name}",
            )

            transformation_pipeline = get_transform_pipeline_group(
                project_name=self.name,
                target_env=target_env,
            )

            ingestion_pipeline >> transformation_pipeline

        return dag
