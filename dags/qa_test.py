"""
    Airflow DAG QA jobs across ADF and Synapse.
"""
from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator
)
from operators.synapse import AzureSynapseRunPipelineOperator

from datetime import datetime, timedelta

REGION = "qa"
DAG_ID = f"{REGION}_data_pipeline"


with DAG(
    dag_id=DAG_ID,
    description=f"Pipeline to trigger QA jobs.",
    start_date=datetime(2024, 10, 27),
    max_active_runs=1,
    catchup=False,
    schedule_interval="@weekly",
    # concurrency=3,
    default_args={
        "owner": "Fiona Tang",
        "depends_on_past": False,
    },
) as dag:
    azure_synapse_conn_id = f"azure_synapse_{REGION}"
    azure_data_factory_conn_id = f"azure_data_factory_{REGION}"
    unprocessed_storage_account = Variable.get(f"unprocessed_storage_account_{REGION}")
    date_start = "{{ data_interval_start.strftime('%Y/%m/%d') }}"
    date_end = "{{ data_interval_end.strftime('%Y/%m/%d') }}"
    region = Variable.get(f"region_{REGION}")

    run_ingest_auth0_logs_task = AzureSynapseRunPipelineOperator(
        task_id="run_ingest_auth0_logs",
        pipeline_name="IngestEventHubData",
        parameters={
            "region": region,
            "path": f"streams/auth0/logs/{date_start}",
            "write_location": "logs/auth0",
            "legacy_event_hub": False,
            "source_name": "auth0_logs",
        },
        azure_synapse_conn_id=azure_synapse_conn_id
    )

    run_ingest_dtx_app_insights_task = AzureSynapseRunPipelineOperator(
        task_id="run_ingest_dtx_app_insights",
        pipeline_name="IngestEventHubData",
        parameters={
            "region": region,
            "path": f"streams/dp2/app_insights/{date_start}",
            "write_location": "telemetry/dp2/app_insights/",
            "legacy_event_hub": False,
            "source_name": "dtx_app_insights",
        },
        azure_synapse_conn_id=azure_synapse_conn_id
    )

    run_ingest_dtx_bot_telemetry_task = AzureSynapseRunPipelineOperator(
        task_id="run_ingest_dtx_bot_telemetry",
        pipeline_name="IngestEventHubData",
        parameters={
            "region": region,
            "path": f"streams/dp2/sensitive/bot_telemetry/{date_start}",
            "write_location": "telemetry/dp2/sensitive/bot_telemetry",
            "legacy_event_hub": False,
            "source_name": "dtx_bot_telemetry",
        },
        azure_synapse_conn_id=azure_synapse_conn_id
    )

    run_import_dp2_capi_task = AzureDataFactoryRunPipelineOperator(
        task_id="run_import_dp2_capi",
        pipeline_name="DP2_CAPI_full_load_75w_TopLevel",
        retries=1,
        azure_data_factory_conn_id=azure_data_factory_conn_id
    )

    run_dp2_capi_landing_to_conformance_task = AzureSynapseRunPipelineOperator(
        task_id="run_dp2_capi_landing_to_conformance",
        pipeline_name="ImportLandedFilesToConformance",
        parameters={
            "region": region,
            "landing_directory": f"Transactional/dp2/capi/full/{date_end}",
            "conformance_directory": f"Transactional/dp2/capi/full/{date_end}",
        },
        azure_synapse_conn_id=azure_synapse_conn_id
    )

    run_dp2_usd_landing_to_conformance_task = AzureSynapseRunPipelineOperator(
        task_id="run_dp2_usd_landing_to_conformance",
        pipeline_name="ImportLandedFilesToConformance",
        parameters={
            "region": region,
            "landing_directory": f"Transactional/dp2/usd/full/{date_end}",
            "conformance_directory": f"Transactional/dp2/usd/full/{date_end}",
        },
        azure_synapse_conn_id=azure_synapse_conn_id
    )

    # Define task dependencies
    run_ingest_auth0_logs_task
    run_ingest_dtx_app_insights_task
    run_ingest_dtx_bot_telemetry_task
    import_dp2_capi = run_import_dp2_capi_task >> run_dp2_capi_landing_to_conformance_task
    import_dp2_usd = run_import_dp2_capi_task >> run_dp2_usd_landing_to_conformance_task
