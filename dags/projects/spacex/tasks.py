# tasks.py
import logging
from re import sub
from typing import Type, Sequence

from airflow.operators.bash import BashOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
import pandas as pd

from core.base import PipelineType
from core.storage import MinIOStorage, MinIOConfig
from core.base import AbstractStorage

from projects.spacex.pipeline import SpaceXPipeline

def get_entity_pipeline_task(entity: str):
    """
    Wrapper to run the SpaceX pipeline for a given entity (e.g., 'launches').
    Used as an Airflow TaskFlow function.
    """
    @task(task_id=f"run_pipeline_{entity}")
    def run_entity_pipeline_task() -> None:
        storage_config=MinIOConfig()
        storage_obj=MinIOStorage(config=storage_config)
        pipeline = SpaceXPipeline(entity, )
        pipeline.run()

    return run_entity_pipeline_task

def get_storage_from_config() -> AbstractStorage:
    config = MinIOConfig()
    return MinIOStorage(config)

def get_spacex_pipeline_obj(entity: str) -> SpaceXPipeline:
    """
    Wrapper to run the SpaceX pipeline for a given entity (e.g., 'launches').
    Used on Airflow TaskFlow function.
    """
    storage_obj=get_storage_from_config()
    return SpaceXPipeline(entity)

def build_spacex_entity_pipeline_task_group(entity: str) -> None:
    """
    Build a task group for the SpaceX pipeline for a given entity.
    This function creates a task group with three tasks: extract, transform, and load.
    Each task is responsible for a specific step in the ETL process.
    """
    pipeline = get_spacex_pipeline_obj(entity)

    @task_group(group_id=f"pipeline_{entity}")
    def pipeline_tasks():        
        @task(task_id=f"extract_{entity}")
        def extract_data():
            raw_data = pipeline.extract()
            if not raw_data:
                raise ValueError("No data extracted.")
            return raw_data

        @task(task_id=f"transform_{entity}")
        def transform_data(raw_data):
            df = pipeline.transform(raw_data)
            if df.empty:
                raise ValueError("Transformation resulted in empty dataframe")
            return df.to_json()

        @task(task_id=f"load_{entity}")
        def load_data(serialized_df):
            df = pd.read_json(serialized_df)
            pipeline.load(df)

        raw_data = extract_data()
        transformed_data = transform_data(raw_data)
        load_data(transformed_data)

    return pipeline_tasks()

def build_spacex_ingestion_pipeline(pipeline_name: str) -> TaskGroup:
    @task_group(group_id=pipeline_name)
    def ingestion_group():

        @task
        def get_entities():
            return [
                "capsules", "cores", "dragons", "history",
                "landpads", "launches", "payloads", "rockets", "ships"
            ]

        @task
        def build_entity_pipeline(entity: str):
            build_spacex_entity_pipeline_task_group(entity)

        # 1. Get list of entities
        entities = get_entities()

        # 2. Dynamically map build tasks
        build_entity_pipeline.expand(entity=entities)

    return ingestion_group()