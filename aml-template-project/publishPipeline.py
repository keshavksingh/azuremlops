############################################ DEPLOYING AML PIPELINE PUBLISH

#Set Environment and Params
cwd = artifacts_dir

SOURCE_STORAGE_ACCOUNT = read_setup_ini(cwd,set_environment_var,'SOURCE_STORAGE_ACCOUNT')
AML_SUBSCRIPTION_ID = read_setup_ini(cwd,set_environment_var,'AML_SUBSCRIPTION_ID')
AML_RESOURCE_GROUP = read_setup_ini(cwd,set_environment_var,'AML_RESOURCE_GROUP')
AML_WORKSPACE = read_setup_ini(cwd,set_environment_var,'AML_WORKSPACE')
SOURCE_READ_SPN = read_setup_ini(cwd,set_environment_var,'SOURCE_READ_SPN')
SOURCE_READ_SPNKEY = read_setup_ini(cwd,set_environment_var,'SOURCE_READ_SPNKEY')
SOURCE_READ_PATH = read_setup_ini(cwd,set_environment_var,'SOURCE_READ_PATH')
AML_STORAGE_EXPERIMENT_ROOT_PATH = read_setup_ini(cwd,set_environment_var,'AML_STORAGE_EXPERIMENT_ROOT_PATH')
AML_STORAGE_EXPERIMENT_DELTA_ROOT_PATH = read_setup_ini(cwd,set_environment_var,'AML_STORAGE_EXPERIMENT_DELTA_ROOT_PATH')
AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH = read_setup_ini(cwd,set_environment_var,'AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH')
AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH = read_setup_ini(cwd,set_environment_var,'AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH')
AML_STORAGE_SPN = read_setup_ini(cwd,set_environment_var,'AML_STORAGE_SPN')
AML_STORAGE_SPNKEY = read_setup_ini(cwd,set_environment_var,'AML_STORAGE_SPNKEY')

AML_LOGGER_APPINSIGHT_ID = read_setup_ini(cwd,set_environment_var,'AML_LOGGER_APPINSIGHT_ID')
AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY = read_setup_ini(cwd,set_environment_var,'AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY')
AML_SUBSCRIPTION_ID = read_setup_ini(cwd,set_environment_var,'AML_SUBSCRIPTION_ID')
AML_RESOURCE_GROUP = read_setup_ini(cwd,set_environment_var,'AML_RESOURCE_GROUP')
AML_WORKSPACE = read_setup_ini(cwd,set_environment_var,'AML_WORKSPACE')

KUSTO_SOURCE_READ_SPN = read_setup_ini(cwd,set_environment_var,'KUSTO_SOURCE_READ_SPN')
KUSTO_SOURCE_READ_SPNKEY = read_setup_ini(cwd,set_environment_var,'KUSTO_SOURCE_READ_SPNKEY')

SQL_READ_SPN = read_setup_ini(cwd,set_environment_var,'SQL_READ_SPN')
SQL_READ_SPNKEY = read_setup_ini(cwd,set_environment_var,'SQL_READ_SPNKEY')
SQL_SERVER_INSTANCE = read_setup_ini(cwd,set_environment_var,'SQL_SERVER_INSTANCE')
AML_STORAGE_EXPERIMENT_SQL_ROOT_PATH = read_setup_ini(cwd,set_environment_var,'AML_STORAGE_EXPERIMENT_SQL_ROOT_PATH')
AML_BATCH_SCORE_OUTPUT_ROOT_PATH = read_setup_ini(cwd,set_environment_var,'AML_BATCH_SCORE_OUTPUT_ROOT_PATH')
MODEL_NAME = read_setup_ini(cwd,set_environment_var,'MODEL_NAME')

CX_GRAPH_LINEAGE_ENDPOINT = read_setup_ini(cwd,set_environment_var,'CX_GRAPH_LINEAGE_ENDPOINT')
CX_GRAPH_LINEAGE_ENDPOINT_KEY = read_setup_ini(cwd,set_environment_var,'CX_GRAPH_LINEAGE_ENDPOINT_KEY')

CX_COSMOS_DATAFRESHNESS_ENDPOINT = read_setup_ini(cwd,set_environment_var,'CX_COSMOS_DATAFRESHNESS_ENDPOINT')
CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY = read_setup_ini(cwd,set_environment_var,'CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY')

ADB_SOURCE_READ_SPN_VALUE = SOURCE_READ_SPN
ADB_SOURCE_READ_SPNKEY_VALUE = SOURCE_READ_SPNKEY
ADB_AML_STORAGE_SPN_VALUE = AML_STORAGE_SPN
ADB_AML_STORAGE_SPNKEY_VALUE = AML_STORAGE_SPNKEY
ADB_AML_LOGGER_APPINSIGHT_ID = AML_LOGGER_APPINSIGHT_ID
ADB_AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY = AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY
ADB_KUSTO_SOURCE_READ_SPN = KUSTO_SOURCE_READ_SPN
ADB_KUSTO_SOURCE_READ_SPNKEY = KUSTO_SOURCE_READ_SPNKEY
ADB_CX_GRAPH_LINEAGE_ENDPOINT = CX_GRAPH_LINEAGE_ENDPOINT
ADB_CX_GRAPH_LINEAGE_ENDPOINT_KEY = CX_GRAPH_LINEAGE_ENDPOINT_KEY


print(f"DEPLOYING AML PIPELINE PUBLISH on {target_amlws}")

import os
from azureml.core import Workspace
from azureml.core.authentication import ServicePrincipalAuthentication

# Set up the Service Principal authentication object
svc_pr = ServicePrincipalAuthentication(
    tenant_id=target_tenantid,
    service_principal_id=platform_client_id,
    service_principal_password=platform_client_secret)

# Get the Azure ML workspace using the Service Principal authentication
workspace = Workspace(workspace_name=target_amlws,subscription_id=target_subscriptionid,resource_group=target_resourcegroup,auth=svc_pr)
keyvault=workspace.get_default_keyvault()

## Read AML WS KEY VAULTS

SOURCE_READ_SPN_VALUE= keyvault.get_secret(name=SOURCE_READ_SPN)
SOURCE_READ_SPNKEY_VALUE= keyvault.get_secret(name=SOURCE_READ_SPNKEY)
AML_STORAGE_SPN_VALUE= keyvault.get_secret(name=AML_STORAGE_SPN)
AML_STORAGE_SPNKEY_VALUE= keyvault.get_secret(name=AML_STORAGE_SPNKEY)
AML_LOGGER_APPINSIGHT_ID= keyvault.get_secret(name=AML_LOGGER_APPINSIGHT_ID)
AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY= keyvault.get_secret(name=AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY)
KUSTO_SOURCE_READ_SPN= keyvault.get_secret(name=KUSTO_SOURCE_READ_SPN)
KUSTO_SOURCE_READ_SPNKEY= keyvault.get_secret(name=KUSTO_SOURCE_READ_SPNKEY)
SQL_READ_SPN= keyvault.get_secret(name=SQL_READ_SPN)
SQL_READ_SPNKEY= keyvault.get_secret(name=SQL_READ_SPNKEY)
CX_GRAPH_LINEAGE_ENDPOINT= keyvault.get_secret(name=CX_GRAPH_LINEAGE_ENDPOINT)
CX_GRAPH_LINEAGE_ENDPOINT_KEY= keyvault.get_secret(name=CX_GRAPH_LINEAGE_ENDPOINT_KEY)
CX_COSMOS_DATAFRESHNESS_ENDPOINT= keyvault.get_secret(name=CX_COSMOS_DATAFRESHNESS_ENDPOINT)
CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY= keyvault.get_secret(name=CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY)

#Pipeline Step
from azureml.core import RunConfiguration
from azureml.core import ScriptRunConfig 
from azureml.core import Experiment
from azureml.core.conda_dependencies import CondaDependencies
from azureml.pipeline.core import Pipeline, PipelineData, PipelineParameter, StepSequence
from azureml.pipeline.steps import PythonScriptStep, SynapseSparkStep, DatabricksStep
from azureml.core.compute import ComputeTarget, DatabricksCompute
from azureml.core.environment import Environment
from azureml.core import Run
from azureml.core.run import Run, _OfflineRun
from azureml.pipeline.core.run import PipelineRun
from azureml.pipeline.core.schedule import ScheduleRecurrence, Schedule,TimeZone
from azureml.core import RunConfiguration
import datetime
import uuid
new_guid = uuid.uuid4()

env = Environment(name="synapseenv")

AML_ATTACHED_SYNAPSE_SPARK = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_SYNAPSE_SPARK')
AML_ATTACHED_SYNAPSE_SPARK_DRIVER_MEMORY = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_SYNAPSE_SPARK_DRIVER_MEMORY')
AML_ATTACHED_SYNAPSE_SPARK_DRIVER_CORES = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_SYNAPSE_SPARK_DRIVER_CORES')
AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_MEMORY = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_MEMORY')
AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_CORES = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_CORES')
AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_INSTANCES = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_INSTANCES')
AML_ATTACHED_ADB_SPARK = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_ADB_SPARK')
AML_ATTACHED_ADB_SPARK_CLUSTERID = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_ADB_SPARK_CLUSTERID')
AML_ATTACHED_COMPUTE_AKS = read_setup_ini(cwd,set_environment_params,'AML_ATTACHED_COMPUTE_AKS')
AML_COMPUTE_VM_CLUSTER = read_setup_ini(cwd,set_environment_params,'AML_COMPUTE_VM_CLUSTER')

run_config = RunConfiguration(framework="pyspark")
run_config.target = AML_ATTACHED_SYNAPSE_SPARK

run_config.spark.configuration["spark.driver.memory"] = AML_ATTACHED_SYNAPSE_SPARK_DRIVER_MEMORY
run_config.spark.configuration["spark.driver.cores"] = AML_ATTACHED_SYNAPSE_SPARK_DRIVER_CORES
run_config.spark.configuration["spark.executor.memory"] = AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_MEMORY 
run_config.spark.configuration["spark.executor.cores"] = AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_CORES 
run_config.spark.configuration["spark.executor.instances"] = AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_INSTANCES

exp = Experiment(workspace=workspace, name="AML-Template-Experiment") 
pipeline_name=target_aml_pipeline_name

pipeline_id = Run.get_context().id

pipeline_experiment_name = "AML-Template-Experiment"
pipeline_schedule_name = "sch_pl_aml-template-experiment-weekly-monday"

BatchRunCorrelationId = PipelineParameter(name='BatchRunCorrelationId', default_value=str(new_guid))
ADFPipeline = PipelineParameter(name='ADFPipeline', default_value='ManualAML')
TriggerName = PipelineParameter(name='TriggerName', default_value='ManualAMLTrigger')
DatafactoryName = PipelineParameter(name='DatafactoryName', default_value='ManualAMLRun')

step_1 = PythonScriptStep(name="PreTrainSynapseSparkDataEngineeringStep",
                          source_directory = cwd,
                          script_name='preTrainDataEngineering.py',
                          compute_target=AML_ATTACHED_SYNAPSE_SPARK,
                          arguments=["--SOURCE_READ_SPN_VALUE", SOURCE_READ_SPN_VALUE,\
                           "--SOURCE_READ_SPNKEY_VALUE", SOURCE_READ_SPNKEY_VALUE,\
                           "--AML_STORAGE_SPN_VALUE", AML_STORAGE_SPN_VALUE,\
                           "--AML_STORAGE_SPNKEY_VALUE", AML_STORAGE_SPNKEY_VALUE,\
                           "--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--SOURCE_READ_PATH", SOURCE_READ_PATH,\
                           "--AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE", AML_STORAGE_EXPERIMENT_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "PreTrainSynapseSparkDataEngineeringStep",\
                           "--RUN_ID", BatchRunCorrelationId,\
                           "--ADFPIPELINE", ADFPipeline,\
                           "--TRIGGERNAME", TriggerName,\
                           "--DATAFACTORYNAME", DatafactoryName,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", CX_GRAPH_LINEAGE_ENDPOINT_KEY],
                          runconfig = run_config,
                          allow_reuse=False)


databricks_compute = ComputeTarget(workspace=workspace, name=AML_ATTACHED_ADB_SPARK)
step_2 = DatabricksStep(name="PreTrainDatabricksSparkKustoDataEngineeringStep",
                          source_directory = cwd,
                          python_script_name='preTrainKustoDataEngineering.py',
                          compute_target=databricks_compute,
                          existing_cluster_id=AML_ATTACHED_ADB_SPARK_CLUSTERID,
                          python_script_params=["--SOURCE_READ_SPN_VALUE", ADB_SOURCE_READ_SPN_VALUE,\
                           "--SOURCE_READ_SPNKEY_VALUE", ADB_SOURCE_READ_SPNKEY_VALUE,\
                           "--AML_STORAGE_SPN_VALUE", ADB_AML_STORAGE_SPN_VALUE,\
                           "--AML_STORAGE_SPNKEY_VALUE", ADB_AML_STORAGE_SPNKEY_VALUE,\
                           "--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--SOURCE_READ_PATH", SOURCE_READ_PATH,\
                           "--AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE", AML_STORAGE_EXPERIMENT_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", ADB_AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", ADB_AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "PreTrainDatabricksSparkKustoDataEngineeringStep",\
                           "--RUN_ID", pipeline_id,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH", AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH,\
                           "--KUSTO_SOURCE_READ_SPN", ADB_KUSTO_SOURCE_READ_SPN,\
                           "--KUSTO_SOURCE_READ_SPNKEY", ADB_KUSTO_SOURCE_READ_SPNKEY,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", ADB_CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", ADB_CX_GRAPH_LINEAGE_ENDPOINT_KEY],
                           permit_cluster_restart=True,
                          allow_reuse=False)


step_3 = PythonScriptStep(name="PreTrainSqlDataEngineeringStep",
                          source_directory = cwd,
                          script_name='preTrainSqlDataEngineering.py',
                          compute_target=AML_ATTACHED_SYNAPSE_SPARK,
                          arguments=["--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--AML_STORAGE_SPN_VALUE", AML_STORAGE_SPN_VALUE,\
                           "--AML_STORAGE_SPNKEY_VALUE", AML_STORAGE_SPNKEY_VALUE,\
                           "--SQL_READ_SPN", SQL_READ_SPN,\
                           "--SQL_READ_SPNKEY", SQL_READ_SPNKEY,\
                           "--SQL_SERVER_INSTANCE", SQL_SERVER_INSTANCE,\
                           "--AML_STORAGE_EXPERIMENT_SQL_ROOT_PATH", AML_STORAGE_EXPERIMENT_SQL_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "PreTrainSqlDataEngineeringStep",\
                           "--RUN_ID", BatchRunCorrelationId,\
                           "--ADFPIPELINE", ADFPipeline,\
                           "--TRIGGERNAME", TriggerName,\
                           "--DATAFACTORYNAME", DatafactoryName,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", CX_GRAPH_LINEAGE_ENDPOINT_KEY],
                          runconfig = run_config,
                          allow_reuse=False)

step_4 = PythonScriptStep(name="PreTrainDataQualityStep",
                          source_directory = cwd,
                          script_name='preTrainDataQuality.py',
                          compute_target=AML_ATTACHED_SYNAPSE_SPARK,
                          arguments=["--SOURCE_READ_SPN_VALUE", SOURCE_READ_SPN_VALUE,\
                           "--SOURCE_READ_SPNKEY_VALUE", SOURCE_READ_SPNKEY_VALUE,\
                           "--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE", AML_STORAGE_EXPERIMENT_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "PreTrainDataQualityStep",\
                           "--RUN_ID", BatchRunCorrelationId,\
                           "--ADFPIPELINE", ADFPipeline,\
                           "--TRIGGERNAME", TriggerName,\
                           "--DATAFACTORYNAME", DatafactoryName,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", CX_GRAPH_LINEAGE_ENDPOINT_KEY],
                          runconfig = run_config,
                          allow_reuse=False)

step_5 = PythonScriptStep(name="TrainAMLModelStep-SparkCompute",
                          source_directory = cwd,
                          script_name='TrainModel-SparkCompute.py',
                          compute_target=AML_ATTACHED_SYNAPSE_SPARK,
                          arguments=["--SOURCE_READ_SPN_VALUE", SOURCE_READ_SPN_VALUE,\
                           "--SOURCE_READ_SPNKEY_VALUE", SOURCE_READ_SPNKEY_VALUE,\
                           "--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE", AML_STORAGE_EXPERIMENT_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "TrainAMLModelStep-SparkCompute",\
                           "--RUN_ID", BatchRunCorrelationId,\
                           "--ADFPIPELINE", ADFPipeline,\
                           "--TRIGGERNAME", TriggerName,\
                           "--DATAFACTORYNAME", DatafactoryName,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", CX_GRAPH_LINEAGE_ENDPOINT_KEY],
                          runconfig = run_config,
                          allow_reuse=False)

from azureml.core.conda_dependencies import CondaDependencies
conda_dep = CondaDependencies()
conda_dep.add_pip_package("mlflow")
conda_dep.add_pip_package("azureml-mlflow")
conda_dep.add_pip_package("mlflow[azureml]")
conda_dep.add_pip_package("adlfs")
conda_dep.add_pip_package("azure-ai-ml")
conda_dep.add_pip_package("azureml-core")
conda_dep.add_pip_package("delta-lake-reader[azure]")
conda_dep.add_pip_package("azure-identity")
conda_dep.add_pip_package("gremlinpython")
conda_dep.add_pip_package("applicationinsights")
conda_dep.add_pip_package("azureml-interpret")
conda_dep.add_pip_package("interpret-community")

run_config = RunConfiguration(conda_dependencies=conda_dep)
run_config.target = AML_ATTACHED_COMPUTE_AKS

step_6 = PythonScriptStep(name="TrainAMLModel-AKSCompute-DeltaRead",
                          source_directory = cwd,
                          script_name='TrainModel-AKSCompute-DeltaRead.py',
                          compute_target=AML_ATTACHED_COMPUTE_AKS,
                          arguments=["--SOURCE_READ_SPN_VALUE", SOURCE_READ_SPN_VALUE,\
                           "--SOURCE_READ_SPNKEY_VALUE", SOURCE_READ_SPNKEY_VALUE,\
                           "--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--AML_STORAGE_EXPERIMENT_DELTA_ROOT_PATH", AML_STORAGE_EXPERIMENT_DELTA_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "TrainAMLModel-AKSCompute-DeltaRead",\
                           "--RUN_ID", BatchRunCorrelationId,\
                           "--ADFPIPELINE", ADFPipeline,\
                           "--TRIGGERNAME", TriggerName,\
                           "--DATAFACTORYNAME", DatafactoryName,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", CX_GRAPH_LINEAGE_ENDPOINT_KEY],
                          runconfig = run_config,
                          allow_reuse=False)


run_config = RunConfiguration()
run_config.target = AML_COMPUTE_VM_CLUSTER

step_7 = PythonScriptStep(name="TrainAMLModel-VMCompute-ParquetRead",
                          source_directory = cwd,
                          script_name='TrainModel-VMCompute-ParquetRead.py',
                          compute_target=AML_COMPUTE_VM_CLUSTER,
                          arguments=["--SOURCE_READ_SPN_VALUE", SOURCE_READ_SPN_VALUE,\
                           "--SOURCE_READ_SPNKEY_VALUE", SOURCE_READ_SPNKEY_VALUE,\
                           "--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH", AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "TrainAMLModel-VMCompute-ParquetRead",\
                           "--RUN_ID", BatchRunCorrelationId,\
                           "--ADFPIPELINE", ADFPipeline,\
                           "--TRIGGERNAME", TriggerName,\
                           "--DATAFACTORYNAME", DatafactoryName,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", CX_GRAPH_LINEAGE_ENDPOINT_KEY],
                          runconfig = run_config,
                          allow_reuse=False)

run_config = RunConfiguration(framework="pyspark")
run_config.target = AML_ATTACHED_SYNAPSE_SPARK

run_config.spark.configuration["spark.driver.memory"] = AML_ATTACHED_SYNAPSE_SPARK_DRIVER_MEMORY
run_config.spark.configuration["spark.driver.cores"] = AML_ATTACHED_SYNAPSE_SPARK_DRIVER_CORES
run_config.spark.configuration["spark.executor.memory"] = AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_MEMORY 
run_config.spark.configuration["spark.executor.cores"] = AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_CORES 
run_config.spark.configuration["spark.executor.instances"] = AML_ATTACHED_SYNAPSE_SPARK_EXECUTOR_INSTANCES

step_8 = PythonScriptStep(name="BatchScoreAMLModelStep",
                          source_directory = cwd,
                          script_name='Score.py',
                          compute_target=AML_ATTACHED_SYNAPSE_SPARK,
                          arguments=["--SOURCE_READ_SPN_VALUE", SOURCE_READ_SPN_VALUE,\
                           "--SOURCE_READ_SPNKEY_VALUE", SOURCE_READ_SPNKEY_VALUE,\
                           "--SOURCE_STORAGE_ACCOUNT_VALUE", SOURCE_STORAGE_ACCOUNT,\
                           "--AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE", AML_STORAGE_EXPERIMENT_ROOT_PATH,\
                           "--AML_BATCH_SCORE_OUTPUT_ROOT_PATH", AML_BATCH_SCORE_OUTPUT_ROOT_PATH,\
                           "--AML_LOGGER_APPINSIGHT_ID", AML_LOGGER_APPINSIGHT_ID,\
                           "--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                           "--AML_SUBSCRIPTION_ID", AML_SUBSCRIPTION_ID,\
                           "--AML_RESOURCE_GROUP", AML_RESOURCE_GROUP,\
                           "--AML_WORKSPACE", AML_WORKSPACE,\
                           "--PIPELINE_ID", pipeline_id,\
                           "--PIPELINE_NAME", pipeline_name,\
                           "--PIPELINE_SCHEDULE_NAME", pipeline_schedule_name,\
                           "--PIPELINE_STEP_NAME", "BatchScoreAMLModelStep",\
                           "--RUN_ID", BatchRunCorrelationId,\
                           "--ADFPIPELINE", ADFPipeline,\
                           "--TRIGGERNAME", TriggerName,\
                           "--DATAFACTORYNAME", DatafactoryName,\
                           "--EXPERIMENT_NAME", pipeline_experiment_name,\
                           "--MODEL_NAME", MODEL_NAME,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT", CX_GRAPH_LINEAGE_ENDPOINT,\
                           "--CX_GRAPH_LINEAGE_ENDPOINT_KEY", CX_GRAPH_LINEAGE_ENDPOINT_KEY,\
                           "--CX_COSMOS_DATAFRESHNESS_ENDPOINT", CX_COSMOS_DATAFRESHNESS_ENDPOINT,\
                           "--CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY", CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY],
                          runconfig = run_config,
                          allow_reuse=False)

step_2.run_after(step_1)
step_3.run_after(step_2)
step_4.run_after(step_3)

step_5.run_after(step_4)
step_6.run_after(step_4)
step_7.run_after(step_4)

step_8.run_after(step_5)
step_8.run_after(step_6)
step_8.run_after(step_7)

pipeline = Pipeline(workspace=workspace,steps=[step_1, step_2, step_3, step_4, step_5, step_6, step_7,step_8])

## For a manual Run Uncomment Below and Test
#pipeline_run = pipeline.submit("TrainMode-Pipeline", regenerate_outputs=True)

published_pipeline = pipeline.publish(name=pipeline_name)