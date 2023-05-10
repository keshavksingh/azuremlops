###Simply Adding the Logger and Util Functions running in the Current Run Context
with open('./core/app_insights_logger.py', 'r') as f:
    script = f.read()
exec(script)

with open('./core/util.py', 'r') as f:
    script = f.read()
exec(script)

###Simply Adding the Lineage and running in the Current Run Context
with open('./core/lineagegraph.py', 'r') as f:
    script = f.read()
exec(script)

#install required packages
package_name = ["azureml-mlflow","azure-ai-ml","mlflow[azureml]","pyarrowfs-adlgen2","azure-identity","raiwidgets","responsibleai","pandas","azureml-interpret","interpret-community"]

install_pip(package_name)

#### Add Data Engineering Script Below
import argparse
import datetime
parser = argparse.ArgumentParser()

parser.add_argument("--SOURCE_READ_SPN_VALUE", type=str, help="SOURCE_READ_SPN_VALUE")
parser.add_argument("--SOURCE_READ_SPNKEY_VALUE", type=str, help="SOURCE_READ_SPNKEY_VALUE")
parser.add_argument("--SOURCE_STORAGE_ACCOUNT_VALUE", type=str, help="SOURCE_STORAGE_ACCOUNT_VALUE")
parser.add_argument("--AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH", type=str, help="AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH")
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
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT")
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT_KEY", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT_KEY")

args = parser.parse_args()

SOURCE_READ_SPN_VALUE = args.SOURCE_READ_SPN_VALUE
SOURCE_READ_SPNKEY_VALUE = args.SOURCE_READ_SPNKEY_VALUE
SOURCE_STORAGE_ACCOUNT_VALUE = args.SOURCE_STORAGE_ACCOUNT_VALUE
AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH = args.AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH
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
CX_GRAPH_LINEAGE_ENDPOINT = args.CX_GRAPH_LINEAGE_ENDPOINT
CX_GRAPH_LINEAGE_ENDPOINT_KEY = args.CX_GRAPH_LINEAGE_ENDPOINT_KEY

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

#from azureml.core import Experiment
#from azureml.core.run import Run
#from azureml.interpret import ExplanationClient
#from interpret.ext.blackbox import MimicExplainer
#from interpret.ext.blackbox import TabularExplainer

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn import datasets
import numpy as np
from azureml.core import Workspace

import pandas as pd
from azure.identity import ClientSecretCredential
import pyarrow.fs
import pyarrowfs_adlgen2

tenant_id="9493c38c-a42d-44cc-baef-40b3ee092c52"

###Simply Adding the Pandas Utils in the Current Run Context
with open('./core/pandasutils.py', 'r') as f:
    script = f.read()
exec(script)

pandas_df = read_from_parquet_as_pandas(SOURCE_STORAGE_ACCOUNT_VALUE,\
                                        SOURCE_READ_SPN_VALUE,\
                                        SOURCE_READ_SPNKEY_VALUE,\
                                        tenant_id,\
                                        AML_STORAGE_EXPERIMENT_PARQUET_ROOT_PATH,\
                                        RUN_ID,\
                                        PIPELINE_STEP_NAME)

pandas_df_x = pandas_df[['sepalLenght','sepalWidth','petalLenght','petalWidth']]
pandas_df_y = pandas_df[['target']]
x=pandas_df_x.values.tolist()
y=pandas_df_y.values.tolist()
flat_list_y = [item for sublist in y for item in sublist]

#X_train, X_test, y_train, y_test = train_test_split(x, flat_list_y, test_size=0.2, random_state=7)
X_train, X_test, y_train, y_test = train_test_split(pandas_df_x, pandas_df_y, test_size=0.2, random_state=7)

model_name = "iris-random-forest-model"

logparams={}
logparams['model_name']=model_name

with mlflow.start_run() as run:
    num_estimators = 100
    mlflow.log_param("num_estimators",num_estimators)
    logparams['num_estimators']=num_estimators
    # train the model
    rf = RandomForestRegressor(n_estimators=num_estimators)
    fit_model = rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    mlflow.sklearn.log_model(rf, model_name)
    
    # log model performance 
    mse = mean_squared_error(y_test, predictions)
    mlflow.log_metric("MSE", mse)
    print("mse: %f" % mse)
    logparams['mse']=mse

    
    # log model performance
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    print("rmse:",rmse)
    mlflow.log_metric("RMSE", rmse)
    logparams['RMSE']=rmse

    run_id = run.info.run_uuid
    experiment_id = run.info.experiment_id
    mlflow.end_run()
    model_uri = mlflow.get_artifact_uri()
    print(model_uri)
    print("runID: %s" % run_id)
    logparams['model_uri']=model_uri
    logparams['mlflow_exp_run_id']=run_id

###### Required to Ensure We Point to the Right Model_URI
#model_uri required format = "azureml://<host>/experiments/<experiment_name>/runs/<run_id>/artifacts/<run_artifact_path>"
artifact_path = model_uri+"/"+model_name
logparams['artifact_path']=artifact_path
new_run_id = run_id
# Extract the old run ID from the artifact path
start_index = artifact_path.find("/runs/") + len("/runs/")
end_index = artifact_path.find("/artifacts")
old_run_id = artifact_path[start_index:end_index]

# Replace the old run ID with the new run ID in the artifact path
new_artifact_path = artifact_path.replace(old_run_id, new_run_id)

### Get The Latest Version If Any Add 1 To have a new Version
version = 1
client = mlflow.tracking.MlflowClient()
for model in client.search_registered_models('iris-random-forest-model'):
    if model.name == model_name:
        latest_version = model.latest_versions[0]
        print(f"Model '{model.name}' version '{latest_version.version}' is located at '{latest_version.source}'")
        print(latest_version.version)
        version = int(latest_version.version)+1
#########

registered_model = mlflow.register_model(model_uri=new_artifact_path,\
                                         name=model_name,\
                                         tags={"version": str(version),"description":"Trained model For iris-random-forest-model"})
print("Model Registered!")
"""
####### RAI

experiment = Experiment(workspace=workspace, name=EXPERIMENT_NAME) 
run = Run(experiment=experiment, run_id=run_id)

explainer = TabularExplainer(rf, 
                            X_train, 
                            features=pandas_df_x.columns)
explainer = MimicExplainer(rf, 
                        X_train, 
                        DecisionTreeExplainableModel, 
                        augment_data=True, 
                        max_num_of_augmentations=10, 
                        features=pandas_df_x.columns)
global_explanation = explainer.explain_global(X_test)

# Get an Explanation Client and upload the explanation
explain_client = ExplanationClient.from_run(run)                 
explain_client.upload_model_explanation(global_explanation, comment='Tabular_Explanation')

##https://github.com/Azure/MachineLearningNotebooks/blob/25baf5203afa9904d8a154a50143184497f7a52c/contrib/fairness/upload-fairness-dashboard.ipynb
sensitivefeatures = X_test[['sepalLenght','sepalWidth','petalLenght','petalWidth']]

from fairlearn.metrics import selection_rate, MetricFrame

sr = MetricFrame(metrics=selection_rate, y_true=y_test, y_pred=df_pred, sensitive_features=sensitivefeatures)
FairnessDashboard(sensitive_features=sensitivefeatures,
                  y_true=y_test,
                  y_pred=df_pred)

from fairlearn.metrics._group_metric_set import _create_group_metric_set
sf = { 'sepalLenght': X_test.sepalLenght, 'sepalWidth': X_test.sepalWidth, 'petalLenght': X_test.petalLenght, 'petalWidth' : X_test.petalWidth}
dash_dict = _create_group_metric_set(y_true=y_test,
                                     predictions=df_pred,
                                     sensitive_features=sf,
                                     prediction_type='regression')

from azureml.contrib.fairness import upload_dashboard_dictionary, download_dashboard_by_upload_id
dashboard_title = "Fairness_Upload"
upload_id = upload_dashboard_dictionary(run,dash_dict,dashboard_name=dashboard_title)
print("\nUploaded to id: {0}\n".format(upload_id))
downloaded_dict = download_dashboard_by_upload_id(run, upload_id)

run.complete()
"""
################## RAI Complete
################## Logging & Lineage

CurrTime = datetime.datetime.now()
EndTime = CurrTime
amlPipelinelogger.trackEvent(telemetryClient,PIPELINE_STEP_NAME,PIPELINE_SCHEDULE_NAME,{**{'Description':PIPELINE_STEP_NAME,\
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
                            'START_TIME':str(StartTime),\
                            'END_TIME':str(EndTime),\
                            'STATUS':'COMPLETED',\
                            'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds())},**logparams},None)

documentId = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', '"+PIPELINE_STEP_NAME+"').values('id')")[0]
LineageLogger.update_vertex(documentId, {**{'END_TIME':str(EndTime),\
                                            'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds())},\
                                            **logparams})

source_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'PreTrainDataQualityStep').values('id')")[0]
target_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', '"+PIPELINE_STEP_NAME+"').values('id')")[0]
LineageLogger.insert_edges(source_v_id, target_v_id,"isFollowedBy", None)