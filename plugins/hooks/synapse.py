from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Union

from azure.core.exceptions import ServiceRequestError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.synapse.artifacts import ArtifactsClient

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import get_field

if TYPE_CHECKING:
    from azure.synapse.artifacts.models import CreateRunResponse

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


class AzureSynapsePipelineRunStatus:
    """Azure Synapse pipeline operation statuses."""

    QUEUED = "Queued"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELING = "Canceling"
    CANCELLED = "Cancelled"
    TERMINAL_STATUSES = {CANCELLED, FAILED, SUCCEEDED}
    INTERMEDIATE_STATES = {QUEUED, IN_PROGRESS, CANCELING}
    FAILURE_STATES = {FAILED, CANCELLED}


class AzureSynapsePipelineRunException(AirflowException):
    """An exception that indicates a pipeline run failed to complete."""


class AzureSynapseHook(BaseHook):
    """
    A hook to interact with Azure Synapse.

    :param azure_synapse_conn_id: The connection identifier for connecting to
        Azure Synapse.
    """

    conn_type: str = "azure_synapse"
    conn_name_attr: str = "azure_synapse_conn_id"
    default_conn_name: str = "azure_synapse_default"
    hook_name: str = "Azure Synapse"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(
                lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "subscriptionId": StringField(
                lazy_gettext("Subscription ID"), widget=BS3TextFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Secret",
                "host": "Synapse Workspace URL"
            },
        }

    def __init__(self, azure_synapse_conn_id: str = default_conn_name):
        self._conn: ArtifactsClient | None = None
        self.conn_id = azure_synapse_conn_id
        super().__init__()

    def _get_field(self, extras, name):
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=name,
        )

    def get_conn(self) -> ArtifactsClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        subscription_id = self._get_field(extras, "subscriptionId")
        if not subscription_id:
            raise ValueError("A Subscription ID is required to connect to "
                             "Azure Synapse.")

        credential: Credentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when "
                                 "authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password,
                tenant_id=tenant
            )
        else:
            credential = DefaultAzureCredential()

        self._conn = self._create_client(credential, conn.host)

        return self._conn

    @staticmethod
    def _create_client(credential: Credentials, endpoint: str):
        return ArtifactsClient(
            endpoint=endpoint, credential=credential
        )

    def refresh_conn(self) -> ArtifactsClient:
        self._conn = None
        return self.get_conn()

    def run_pipeline(
        self,
        pipeline_name: str,
        **config: Any,
    ) -> CreateRunResponse:
        """
        Run a pipeline.

        :param pipeline_name: The pipeline name.
        :param config: Extra parameters for the client.
        :return: The pipeline run.
        """
        return self.get_conn().pipeline.create_pipeline_run(
            pipeline_name, **config
        )

    def get_pipeline_run_status(
        self,
        run_id: str,
    ) -> str:
        """
        Get a pipeline run's current status.

        :param run_id: The pipeline run identifier.
        :return: The status of the pipeline run.
        """
        self.log.info("Getting the status of run ID %s.", run_id)
        pipeline_run_status = self.get_conn().pipeline_run.get_pipeline_run(
            run_id=run_id).status
        self.log.info(
            "Current status of pipeline run %s: %s",
            run_id, pipeline_run_status
        )

        return pipeline_run_status

    def wait_for_pipeline_run_status(
        self,
        run_id: str,
        expected_statuses: str | set[str],
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Waits for a pipeline run to match an expected status.

        :param run_id: The pipeline run identifier.
        :param expected_statuses: The desired status(es) to check against a
            pipeline run's current status.
        :param check_interval: Time in seconds to check on a pipeline run's
            status.
        :param timeout: Time in seconds to wait for a pipeline to reach a
            terminal status or the expected status.
        :return: Boolean indicating if the pipeline run has reached the
            ``expected_status``.
        """
        pipeline_run_status = self.get_pipeline_run_status(run_id)
        executed_after_token_refresh = True

        start_time = time.monotonic()

        while (
            pipeline_run_status not in AzureSynapsePipelineRunStatus.TERMINAL_STATUSES
            and pipeline_run_status not in expected_statuses
        ):
            # Check if the pipeline-run duration has exceeded the
            # ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise AzureSynapsePipelineRunException(
                    f"Pipeline run {run_id} has not reached a terminal "
                    f"status after {timeout} seconds."
                )

            # Wait to check the status of the pipeline run based on the
            # ``check_interval`` configured.
            time.sleep(check_interval)

            try:
                pipeline_run_status = self.get_pipeline_run_status(run_id)
                executed_after_token_refresh = True
            except ServiceRequestError:
                if executed_after_token_refresh:
                    self.refresh_conn()
                else:
                    raise

        return pipeline_run_status in expected_statuses

    def cancel_pipeline_run(
        self,
        run_id: str,
        is_recursive: bool = True,
        **config: Any,
    ) -> None:
        """
        Cancel the pipeline run.

        :param run_id: The pipeline run identifier.
        :param is_recursive: If true, cancel all the Child pipelines that are
            triggered by the current pipeline. Default is true.
        :param config: Extra parameters for the client.
        """
        self.get_conn().pipeline_run.cancel_pipeline_run(
            run_id=run_id, is_recursive=is_recursive, **config
        )
