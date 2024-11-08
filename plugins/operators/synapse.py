from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.models import BaseOperator

from hooks.synapse import (
    AzureSynapseHook,
    AzureSynapsePipelineRunStatus,
    AzureSynapsePipelineRunException,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureSynapseRunPipelineOperator(BaseOperator):
    """
    Executes a Synapse pipeline.

    :param azure_synapse_conn_id: The connection identifier for connecting to
        Azure Synapse.
    :param pipeline_name: The name of the pipeline to execute.
    :param wait_for_termination: Flag to wait on a pipeline run's termination.
        By default, this feature is enabled but could be disabled to perform
        an asynchronous wait for a long-running pipeline execution using
        a sensor. Note: Asynchronous wait has not been implemented currently.
    :param reference_pipeline_run_id: The pipeline run identifier. If this
        run ID is specified the parameters of the specified run will be used
        to create a new run.
    :param is_recovery: Recovery mode flag. If recovery mode is set to `True`,
        the specified referenced pipeline run and the new run will be grouped
        under the same ``groupId``.
    :param start_activity_name: In recovery mode, the rerun will start from
        this activity. If not specified, all activities will run.
    :param parameters: Parameters of the pipeline run. These parameters are
        referenced in a pipeline via ``@pipeline().parameters.parameterName``
        and will be used only if the ``reference_pipeline_run_id`` is
        not specified.
    :param timeout: Time in seconds to wait for a pipeline to reach a terminal
        status for non-asynchronous waits. Used only if `
        `wait_for_termination`` is True.
    :param check_interval: Time in seconds to check on a pipeline run's status
        for non-asynchronous waits. Used only if ``wait_for_termination``
        is True.
    :param deferrable: Run operator in deferrable mode. Note: this is not
        currently implemented.
    """

    template_fields: Sequence[str] = (
        "azure_synapse_conn_id",
        "pipeline_name",
        "reference_pipeline_run_id",
        "parameters",
    )
    template_fields_renderers = {"parameters": "json"}

    ui_color = "#51e7ff"

    def __init__(
            self,
            *,
            pipeline_name: str,
            azure_synapse_conn_id: str = AzureSynapseHook.default_conn_name,
            wait_for_termination: bool = True,
            reference_pipeline_run_id: str | None = None,
            is_recovery: bool | None = None,
            start_activity_name: str | None = None,
            parameters: dict[str, Any] | None = None,
            timeout: int = 60 * 60 * 24 * 7,
            check_interval: int = 60,
            deferrable: bool = conf.getboolean("operators",
                                               "default_deferrable",
                                               fallback=False),
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.azure_synapse_conn_id = azure_synapse_conn_id
        self.pipeline_name = pipeline_name
        self.wait_for_termination = wait_for_termination
        self.reference_pipeline_run_id = reference_pipeline_run_id
        self.is_recovery = is_recovery
        self.start_activity_name = start_activity_name
        self.parameters = parameters
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable

    def hook(self) -> AzureSynapseHook:
        """Create and return an AzureSynapseHook"""
        return AzureSynapseHook(
            azure_synapse_conn_id=self.azure_synapse_conn_id)

    def execute(self, context: Context) -> None:
        self.log.info(
            "Executing the %s pipeline.", self.pipeline_name
        )
        synapse_hook = self.hook()
        response = synapse_hook.run_pipeline(
            pipeline_name=self.pipeline_name,
            reference_pipeline_run_id=self.reference_pipeline_run_id,
            is_recovery=self.is_recovery,
            start_activity_name=self.start_activity_name,
            parameters=self.parameters,
        )
        self.run_id = vars(response)["run_id"]
        context["ti"].xcom_push(key="run_id", value=self.run_id)

        if self.wait_for_termination:
            self.log.info("Waiting for pipeline run %s to terminate.",
                          self.run_id)

            if synapse_hook.wait_for_pipeline_run_status(
                    run_id=self.run_id,
                    expected_statuses=AzureSynapsePipelineRunStatus.SUCCEEDED,
                    check_interval=self.check_interval,
                    timeout=self.timeout,
            ):
                self.log.info("Pipeline run %s has completed "
                              "successfully.", self.run_id)
            else:
                raise AzureSynapsePipelineRunException(
                    f"Pipeline run {self.run_id} has failed or has been "
                    f"cancelled."
                )
        else:
            if self.deferrable is True:
                warnings.warn(
                    "Argument `wait_for_termination` is False and "
                    "`deferrable` is True , hence"
                    "`deferrable` parameter doesn't have any effect",
                )

    def on_kill(self) -> None:
        if self.run_id:
            synapse_hook = self.hook()
            synapse_hook.cancel_pipeline_run(
                run_id=self.run_id,
            )

            # Check to ensure the pipeline run was cancelled as expected.
            if synapse_hook.wait_for_pipeline_run_status(
                    run_id=self.run_id,
                    expected_statuses=AzureSynapsePipelineRunStatus.CANCELLED,
                    check_interval=self.check_interval,
                    timeout=self.timeout,
            ):
                self.log.info(
                    "Pipeline run %s has been cancelled successfully.",
                    self.run_id)
            else:
                raise AzureSynapsePipelineRunException(
                    f"Pipeline run {self.run_id} was not cancelled."
                )
