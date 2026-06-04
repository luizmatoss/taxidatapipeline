import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupRestartPolicy,
    EnvironmentVariable,
    ImageRegistryCredential,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
)

AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
AZURE_RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP")
AZURE_LOCATION = os.getenv("AZURE_LOCATION", "eastus")

ACI_CONTAINER_GROUP_NAME = os.getenv("ACI_CONTAINER_GROUP_NAME", "taxi-pipeline-job")
ACI_IMAGE = os.getenv("ACI_IMAGE")
ACI_CONTAINER_NAME = os.getenv("ACI_CONTAINER_NAME", "pipeline")
ACI_COMMAND = os.getenv("ACI_COMMAND", "python main.py")
ACI_CPU = float(os.getenv("ACI_CPU", "2.0"))
ACI_MEMORY_GB = float(os.getenv("ACI_MEMORY_GB", "4.0"))

BRONZE_PATH = os.getenv("BRONZE_PATH", "/mnt/data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/mnt/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/mnt/data/gold")

ACR_LOGIN_SERVER = os.getenv("AZURE_REGISTRY_LOGIN_SERVER")
ACR_USERNAME = os.getenv("AZURE_REGISTRY_USERNAME")
ACR_PASSWORD = os.getenv("AZURE_REGISTRY_PASSWORD")

POLL_INTERVAL_SECONDS = int(os.getenv("ACI_POLL_INTERVAL_SECONDS", "15"))
POLL_TIMEOUT_SECONDS = int(os.getenv("ACI_POLL_TIMEOUT_SECONDS", "7200"))


def _validate_required_env() -> None:
    missing = []
    if not AZURE_SUBSCRIPTION_ID:
        missing.append("AZURE_SUBSCRIPTION_ID")
    if not AZURE_RESOURCE_GROUP:
        missing.append("AZURE_RESOURCE_GROUP")
    if not ACI_IMAGE:
        missing.append("ACI_IMAGE")
    if missing:
        raise AirflowException(
            f"Missing required environment variables: {', '.join(missing)}"
        )


def submit_aci_container(**context) -> None:
    _validate_required_env()

    credential = DefaultAzureCredential()
    client = ContainerInstanceManagementClient(credential, AZURE_SUBSCRIPTION_ID)

    env_vars = [
        EnvironmentVariable(name="BRONZE_PATH", value=BRONZE_PATH),
        EnvironmentVariable(name="SILVER_PATH", value=SILVER_PATH),
        EnvironmentVariable(name="GOLD_PATH", value=GOLD_PATH),
    ]

    container = Container(
        name=ACI_CONTAINER_NAME,
        image=ACI_IMAGE,
        command=ACI_COMMAND.split(),
        environment_variables=env_vars,
        resources=ResourceRequirements(
            requests=ResourceRequests(cpu=ACI_CPU, memory_in_gb=ACI_MEMORY_GB)
        ),
    )

    image_registry_credentials = None
    if ACR_LOGIN_SERVER and ACR_USERNAME and ACR_PASSWORD:
        image_registry_credentials = [
            ImageRegistryCredential(
                server=ACR_LOGIN_SERVER,
                username=ACR_USERNAME,
                password=ACR_PASSWORD,
            )
        ]

    group = ContainerGroup(
        location=AZURE_LOCATION,
        containers=[container],
        os_type=OperatingSystemTypes.linux,
        restart_policy=ContainerGroupRestartPolicy.never,
        image_registry_credentials=image_registry_credentials,
    )

    client.container_groups.begin_create_or_update(
        resource_group_name=AZURE_RESOURCE_GROUP,
        container_group_name=ACI_CONTAINER_GROUP_NAME,
        container_group=group,
    ).result()

    context["ti"].xcom_push(key="container_group_name", value=ACI_CONTAINER_GROUP_NAME)


def poll_aci_completion(**context) -> None:
    _validate_required_env()

    credential = DefaultAzureCredential()
    client = ContainerInstanceManagementClient(credential, AZURE_SUBSCRIPTION_ID)

    container_group_name = context["ti"].xcom_pull(
        task_ids="submit_aci_container",
        key="container_group_name",
    )
    if not container_group_name:
        raise AirflowException("No container group name found in XCom.")

    start_time = time.time()

    while True:
        if time.time() - start_time > POLL_TIMEOUT_SECONDS:
            raise AirflowException("Timed out waiting for ACI container completion.")

        group = client.container_groups.get(AZURE_RESOURCE_GROUP, container_group_name)

        if not group.containers or not group.containers[0].instance_view:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        current_state = group.containers[0].instance_view.current_state
        if not current_state:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        state = (current_state.state or "").lower()

        if state == "terminated":
            exit_code = current_state.exit_code
            if exit_code == 0:
                return

            try:
                logs_obj = client.containers.list_logs(
                    resource_group_name=AZURE_RESOURCE_GROUP,
                    container_group_name=container_group_name,
                    container_name=ACI_CONTAINER_NAME,
                    tail=200,
                )
                logs_text = getattr(logs_obj, "content", "") or ""
            except Exception:
                logs_text = ""

            raise AirflowException(
                f"ACI container terminated with non-zero exit code {exit_code}. Logs:\n{logs_text}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="taxi_pipeline_aci",
    description="Submit Taxi pipeline container to ACI and poll to completion",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["azure", "aci", "taxi-pipeline"],
):
    submit = PythonOperator(
        task_id="submit_aci_container",
        python_callable=submit_aci_container,
    )

    poll = PythonOperator(
        task_id="poll_aci_completion",
        python_callable=poll_aci_completion,
    )

    submit >> poll
