###Simply Adding the Logger and Util Functions running in the Current Run Context
with open('./core/app_insights_logger.py', 'r') as f:
    script = f.read()
exec(script)

###Simply Adding the Lineage and running in the Current Run Context
with open('./core/lineagegraph.py', 'r') as f:
    script = f.read()
exec(script)

#### Add Data Engineering Script Below
import argparse
import datetime
parser = argparse.ArgumentParser()

parser.add_argument("--SOURCE_READ_SPN_VALUE", type=str, help="SOURCE_READ_SPN_VALUE")
parser.add_argument("--SOURCE_READ_SPNKEY_VALUE", type=str, help="SOURCE_READ_SPNKEY_VALUE")
parser.add_argument("--SOURCE_STORAGE_ACCOUNT_VALUE", type=str, help="SOURCE_STORAGE_ACCOUNT_VALUE")
parser.add_argument("--AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE", type=str, help="AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE")
parser.add_argument("--AML_BATCH_SCORE_OUTPUT_ROOT_PATH", type=str, help="AML_BATCH_SCORE_OUTPUT_ROOT_PATH")
parser.add_argument("--AML_LOGGER_APPINSIGHT_ID", type=str, help="AML_LOGGER_APPINSIGHT_ID")
parser.add_argument("--AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY", type=str, help="AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY")
parser.add_argument("--AML_SUBSCRIPTION_ID", type=str, help="AML_SUBSCRIPTION_ID")
parser.add_argument("--AML_RESOURCE_GROUP", type=str, help="AML_RESOURCE_GROUP")
parser.add_argument("--AML_WORKSPACE", type=str, help="AML_WORKSPACE")
parser.add_argument("--PIPELINE_ID", type=str, help="PIPELINE_ID")
parser.add_argument("--PIPELINE_NAME", type=str, help="PIPELINE_NAME")
parser.add_argument("--PIPELINE_SCHEDULE_NAME", type=str, help="PIPELINE_SCHEDULE_NAME")
parser.add_argument("--PIPELINE_STEP_NAME", type=str, help="PIPELINE_STEP_NAME")
parser.add_argument("--RUN_ID", type=str, help="RUN_ID")
parser.add_argument("--ADFPIPELINE", type=str, help="ADFPIPELINE")
parser.add_argument("--TRIGGERNAME", type=str, help="TRIGGERNAME")
parser.add_argument("--DATAFACTORYNAME", type=str, help="DATAFACTORYNAME")
parser.add_argument("--EXPERIMENT_NAME", type=str, help="EXPERIMENT_NAME")
parser.add_argument("--MODEL_NAME", type=str, help="MODEL_NAME")
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT")
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT_KEY", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT_KEY")
parser.add_argument("--CX_COSMOS_DATAFRESHNESS_ENDPOINT", type=str, help="CX_COSMOS_DATAFRESHNESS_ENDPOINT")
parser.add_argument("--CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY", type=str, help="CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY")

args = parser.parse_args()

SOURCE_READ_SPN_VALUE = args.SOURCE_READ_SPN_VALUE
SOURCE_READ_SPNKEY_VALUE = args.SOURCE_READ_SPNKEY_VALUE
SOURCE_STORAGE_ACCOUNT_VALUE = args.SOURCE_STORAGE_ACCOUNT_VALUE
AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE = args.AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE
AML_BATCH_SCORE_OUTPUT_ROOT_PATH = args.AML_BATCH_SCORE_OUTPUT_ROOT_PATH
AML_LOGGER_APPINSIGHT_ID = args.AML_LOGGER_APPINSIGHT_ID
AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY = args.AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY
AML_SUBSCRIPTION_ID = args.AML_SUBSCRIPTION_ID
AML_RESOURCE_GROUP = args.AML_RESOURCE_GROUP
AML_WORKSPACE = args.AML_WORKSPACE
PIPELINE_ID = args.PIPELINE_ID
PIPELINE_NAME = args.PIPELINE_NAME
PIPELINE_SCHEDULE_NAME = args.PIPELINE_SCHEDULE_NAME
PIPELINE_STEP_NAME = args.PIPELINE_STEP_NAME
RUN_ID = args.RUN_ID
ADFPIPELINE = args.ADFPIPELINE
TRIGGERNAME = args.TRIGGERNAME
DATAFACTORYNAME = args.DATAFACTORYNAME
EXPERIMENT_NAME = args.EXPERIMENT_NAME
MODEL_NAME = args.MODEL_NAME
CX_GRAPH_LINEAGE_ENDPOINT = args.CX_GRAPH_LINEAGE_ENDPOINT
CX_GRAPH_LINEAGE_ENDPOINT_KEY = args.CX_GRAPH_LINEAGE_ENDPOINT_KEY
CX_COSMOS_DATAFRESHNESS_ENDPOINT = args.CX_COSMOS_DATAFRESHNESS_ENDPOINT
CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY = args.CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY


amlPipelinelogger= telemetrylogger(AML_LOGGER_APPINSIGHT_ID,\
                                    AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                                    AML_SUBSCRIPTION_ID,\
                                    EXPERIMENT_NAME,\
                                    RUN_ID)

telemetryClient = amlPipelinelogger.NewTelemetryClient()

CurrTime = datetime.datetime.now()
StartTime = CurrTime
amlPipelinelogger.trackEvent(telemetryClient,PIPELINE_STEP_NAME,PIPELINE_SCHEDULE_NAME,{'Description':PIPELINE_STEP_NAME,\
                            'AML_SUBSCRIPTION_ID':AML_SUBSCRIPTION_ID,\
                            'AML_RESOURCE_GROUP':AML_RESOURCE_GROUP,\
                            'AML_WORKSPACE':AML_WORKSPACE,\
                            'PIPELINE_ID':PIPELINE_ID,\
                            'PIPELINE_NAME':PIPELINE_NAME,\
                            'PIPELINE_SCHEDULE_NAME':PIPELINE_SCHEDULE_NAME,\
                            'PIPELINE_STEP_NAME':PIPELINE_STEP_NAME,\
                            'RUN_ID':RUN_ID,\
                            'ADFPIPELINE':ADFPIPELINE,\
                            'TRIGGERNAME':TRIGGERNAME,\
                            'DATAFACTORYNAME':DATAFACTORYNAME,\
                            'EXPERIMENT_NAME':EXPERIMENT_NAME,\
                            'MODEL_NAME':MODEL_NAME,\
                            'START_TIME':str(StartTime),\
                            'STATUS':'STARTED'},None)

LineageLogger = LineageGraph(CX_GRAPH_LINEAGE_ENDPOINT, CX_GRAPH_LINEAGE_ENDPOINT_KEY,"EXPERIMENT_NAME")

LineageLogger.add_vertex("amlrun", {"RUN_ID":RUN_ID,\
                            'AML_SUBSCRIPTION_ID':AML_SUBSCRIPTION_ID,\
                            'AML_RESOURCE_GROUP':AML_RESOURCE_GROUP,\
                            'AML_WORKSPACE':AML_WORKSPACE,\
                            'PIPELINE_ID':PIPELINE_ID,\
                            'PIPELINE_NAME':PIPELINE_NAME,\
                            'PIPELINE_SCHEDULE_NAME':PIPELINE_SCHEDULE_NAME,\
                            'PIPELINE_STEP_NAME':PIPELINE_STEP_NAME,\
                            'ADFPIPELINE':ADFPIPELINE,\
                            'TRIGGERNAME':TRIGGERNAME,\
                            'DATAFACTORYNAME':DATAFACTORYNAME,\
                            'EXPERIMENT_NAME':EXPERIMENT_NAME,\
                            'START_TIME':str(StartTime)})

DATAFRSHNESS_LOG={"id":RUN_ID,\
                            "RUN_ID":RUN_ID,\
                            'AML_SUBSCRIPTION_ID':AML_SUBSCRIPTION_ID,\
                            'AML_RESOURCE_GROUP':AML_RESOURCE_GROUP,\
                            'AML_WORKSPACE':AML_WORKSPACE,\
                            'PIPELINE_ID':PIPELINE_ID,\
                            'PIPELINE_NAME':PIPELINE_NAME,\
                            'PIPELINE_SCHEDULE_NAME':PIPELINE_SCHEDULE_NAME,\
                            'PIPELINE_STEP_NAME':PIPELINE_STEP_NAME,\
                            'ADFPIPELINE':ADFPIPELINE,\
                            'TRIGGERNAME':TRIGGERNAME,\
                            'DATAFACTORYNAME':DATAFACTORYNAME,\
                            'EXPERIMENT_NAME':EXPERIMENT_NAME,\
                            'START_TIME':str(StartTime)}

##Score Model

import datetime
import os
import mlflow
import mlflow.sklearn
from sklearn import datasets
import numpy as np
import azureml.core
from azureml.core import Workspace, Datastore
from azureml.core.model import Model
from azureml.core import Workspace, Model
import pandas as pd

tenant_id="9493c38c-a42d-44cc-baef-40b3ee092c52"

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()

spark = SparkSession(sc)

###Simply Adding the Spark Utils in the Current Run Context
with open('./core/sparkutils.py', 'r') as f:
    script = f.read()
exec(script)
##### Read Data
df = read_from_adls_gen2(SOURCE_STORAGE_ACCOUNT_VALUE,\
                         AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE,\
                         'parquet',\
                         SOURCE_READ_SPN_VALUE,\
                         SOURCE_READ_SPNKEY_VALUE,\
                         RUN_ID,\
                         PIPELINE_STEP_NAME)

pandas_df = df.toPandas()
pandas_df_x = pandas_df[['sepalLenght','sepalWidth','petalLenght','petalWidth']]
pandas_df_y = pandas_df[['target']]
x=pandas_df_x.values.tolist()
df = df.drop('Target')
#print(x)

#ws = Workspace(subscription_id=AML_SUBSCRIPTION_ID, resource_group=AML_RESOURCE_GROUP, workspace_name=AML_WORKSPACE)

# Set the registered model name and version
model_name = MODEL_NAME
model_version = 1

client = mlflow.tracking.MlflowClient()
for model in client.search_registered_models(MODEL_NAME):
    if model.name == model_name:
        latest_version = model.latest_versions[0]
        print(f"Model '{model.name}' version '{latest_version.version}' is located at '{latest_version.source}'")
        print(latest_version.version)
        model_version = latest_version.version
        mlflow_modelSourceUri = latest_version.source
        

# Load the registered model
#model = Model(ws, model_name, version=model_version)
#registered_model = Model(ws, name=model_name, version=model_version)

# Load the MLflow model as a PySpark transformer
##runs:/<run_id>/run-relative/path/to/artifact'
#mlflow_model_uri = "azureml://experiments/TrainMode-Pipeline/runs/df219962-999d-4158-90f9-de4298add13a/artifacts/iris-random-forest-model"
#mlflow_model_uri = "runs://df219962-999d-4158-90f9-de4298add13a/artifacts/iris-random-forest-model"
#predict_function = mlflow.pyfunc.spark_udf(spark, mlflow_model_uri, result_type='double') 

pyfunc = mlflow.pyfunc.load_model(mlflow_modelSourceUri)
#df = df.withColumn("predictions", predict_function(*df.columns))

# Perform batch scoring on the data
#predictions = model.transform(df)
#predictions = pyfunc.predict(x)
#print(predictions)

pandas_df_x["predicted_data"] = pandas_df_x.apply(lambda row: pyfunc.predict([[row[col] for col in pandas_df_x.columns]]), axis=1)
predicted_values = pandas_df_x["predicted_data"].apply(lambda x: x[0])
pandas_df_x["predicted_data"] = predicted_values
print(pandas_df_x)

#df_pandas_target = pd.DataFrame(predictions, columns=['Target'])
# convert Pandas dataframe to Spark dataframe
dfScored = spark.createDataFrame(pandas_df_x)
#dfScored.show()

#Write The Output in Final Delta Format.
write_to_adls_gen2(SOURCE_STORAGE_ACCOUNT_VALUE,\
                   AML_BATCH_SCORE_OUTPUT_ROOT_PATH,\
                   'delta',\
                   200,\
                   dfScored,\
                   SOURCE_READ_SPN_VALUE,\
                   SOURCE_READ_SPNKEY_VALUE,\
                   RUN_ID,\
                   PIPELINE_STEP_NAME)
                   
RecordCount = dfScored.count()

CurrTime = datetime.datetime.now()
EndTime = CurrTime

amlPipelinelogger.trackEvent(telemetryClient,PIPELINE_STEP_NAME,PIPELINE_SCHEDULE_NAME,{'Description':PIPELINE_STEP_NAME,\
                            'AML_SUBSCRIPTION_ID':AML_SUBSCRIPTION_ID,\
                            'AML_RESOURCE_GROUP':AML_RESOURCE_GROUP,\
                            'AML_WORKSPACE':AML_WORKSPACE,\
                            'PIPELINE_ID':PIPELINE_ID,\
                            'PIPELINE_NAME':PIPELINE_NAME,\
                            'PIPELINE_SCHEDULE_NAME':PIPELINE_SCHEDULE_NAME,\
                            'PIPELINE_STEP_NAME':PIPELINE_STEP_NAME,\
                            'RUN_ID':RUN_ID,\
                            'ADFPIPELINE':ADFPIPELINE,\
                            'TRIGGERNAME':TRIGGERNAME,\
                            'DATAFACTORYNAME':DATAFACTORYNAME,\
                            'EXPERIMENT_NAME':EXPERIMENT_NAME,\
                            'MODEL_NAME':MODEL_NAME,\
                            'MODEL_VERSION':model_version,\
                            'MODEL_URI':mlflow_modelSourceUri,\
                            'READ_ROOT_PATH_FOR_SCORING':AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE,\
                            'WRITE_ROOT_PATH_AFTER_SCORING':AML_BATCH_SCORE_OUTPUT_ROOT_PATH,\
                            'START_TIME':str(StartTime),\
                            'END_TIME':str(EndTime),\
                            'STATUS':'COMPLETED',\
                            'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds())},{'ScoredRecordsCount':RecordCount})

DATAFRSHNESS_LOG = {**DATAFRSHNESS_LOG,**{'END_TIME':str(EndTime),\
                                    'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds()),\
                                    'MODEL_NAME':MODEL_NAME,\
                                    'MODEL_VERSION':model_version,\
                                    'MODEL_URI':mlflow_modelSourceUri,\
                                    'READ_ROOT_PATH_FOR_SCORING':AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE,\
                                    'WRITE_ROOT_PATH_AFTER_SCORING':AML_BATCH_SCORE_OUTPUT_ROOT_PATH,\
                                    'SCORED_RECORD_COUNT':RecordCount}}

documentId = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', '"+PIPELINE_STEP_NAME+"').values('id')")[0]
LineageLogger.update_vertex(documentId, {'END_TIME':str(EndTime),\
                                    'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds()),\
                                    'MODEL_NAME':MODEL_NAME,\
                                    'MODEL_VERSION':model_version,\
                                    'MODEL_URI':mlflow_modelSourceUri,\
                                    'READ_ROOT_PATH_FOR_SCORING':AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE,\
                                    'WRITE_ROOT_PATH_AFTER_SCORING':AML_BATCH_SCORE_OUTPUT_ROOT_PATH,\
                                    'ScoredRecordsCount':RecordCount})

source_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'TrainAMLModelStep-SparkCompute').values('id')")[0]
source_v_id2 = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'TrainAMLModel-AKSCompute-DeltaRead').values('id')")[0]
source_v_id3 = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'TrainAMLModel-VMCompute-ParquetRead').values('id')")[0]
target_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', '"+PIPELINE_STEP_NAME+"').values('id')")[0]
LineageLogger.insert_edges(source_v_id, target_v_id,"isFollowedBy", None)
LineageLogger.insert_edges(source_v_id2, target_v_id,"isFollowedBy", None)
LineageLogger.insert_edges(source_v_id3, target_v_id,"isFollowedBy", None)

############### ADD DataFreshness
###Simply Adding the Util Functions in the Current Run Context
with open('./core/util.py', 'r') as f:
    script = f.read()
exec(script)

package_name = ["azure-cosmos"]
install_pip(package_name)

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors

DATABASE_ID = 'mlopstelemetrydb'
CONTAINER_ID = 'datafreshness'

client = cosmos_client.CosmosClient(CX_COSMOS_DATAFRESHNESS_ENDPOINT, {'masterKey': CX_COSMOS_DATAFRESHNESS_ENDPOINT_KEY})
database = client.get_database_client(DATABASE_ID)
container = database.get_container_client(CONTAINER_ID)

try:
    container.create_item(body=DATAFRSHNESS_LOG)
    print('Document added successfully.')
except errors.CosmosHttpResponseError as e:
    print('Error adding document: {}'.format(e.message))
######### DataFreshness Added