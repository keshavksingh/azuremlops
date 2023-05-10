######## Collect the Params

import argparse
import datetime
parser = argparse.ArgumentParser()

parser.add_argument("--SOURCE_READ_SPN_VALUE", type=str, help="SOURCE_READ_SPN_VALUE")
parser.add_argument("--SOURCE_READ_SPNKEY_VALUE", type=str, help="SOURCE_READ_SPNKEY_VALUE")
parser.add_argument("--AML_STORAGE_SPN_VALUE", type=str, help="AML_STORAGE_SPN_VALUE")
parser.add_argument("--AML_STORAGE_SPNKEY_VALUE", type=str, help="AML_STORAGE_SPNKEY_VALUE")
parser.add_argument("--SOURCE_STORAGE_ACCOUNT_VALUE", type=str, help="SOURCE_STORAGE_ACCOUNT_VALUE")
parser.add_argument("--SOURCE_READ_PATH", type=str, help="SOURCE_READ_PATH")
parser.add_argument("--AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE", type=str, help="AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE")
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
parser.add_argument("--EXPERIMENT_NAME", type=str, help="EXPERIMENT_NAME")
parser.add_argument("--AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH", type=str, help="AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH")
parser.add_argument("--KUSTO_SOURCE_READ_SPN", type=str, help="KUSTO_SOURCE_READ_SPN")
parser.add_argument("--KUSTO_SOURCE_READ_SPNKEY", type=str, help="KUSTO_SOURCE_READ_SPNKEY")
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT")
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT_KEY", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT_KEY")

#### Mandatory ADB Params Passed by the AML WS By DEFAULT --MUST BE COLLECTED
parser.add_argument("--AZUREML_SCRIPT_DIRECTORY_NAME", type=str, help="AZUREML_SCRIPT_DIRECTORY_NAME")
parser.add_argument("--AZUREML_RUN_TOKEN", type=str, help="AZUREML_RUN_TOKEN")
parser.add_argument("--AZUREML_RUN_TOKEN_EXPIRY", type=str, help="AZUREML_RUN_TOKEN_EXPIRY")
parser.add_argument("--AZUREML_RUN_ID", type=str, help="AZUREML_RUN_ID")
parser.add_argument("--AZUREML_ARM_SUBSCRIPTION", type=str, help="AZUREML_ARM_SUBSCRIPTION")
parser.add_argument("--AZUREML_ARM_RESOURCEGROUP", type=str, help="AZUREML_ARM_RESOURCEGROUP")
parser.add_argument("--AZUREML_ARM_WORKSPACE_NAME", type=str, help="AZUREML_ARM_WORKSPACE_NAME")
parser.add_argument("--AZUREML_ARM_PROJECT_NAME", type=str, help="AZUREML_ARM_PROJECT_NAME")
parser.add_argument("--AZUREML_SERVICE_ENDPOINT", type=str, help="AZUREML_SERVICE_ENDPOINT")
parser.add_argument("--AZUREML_WORKSPACE_ID", type=str, help="AZUREML_WORKSPACE_ID")
parser.add_argument("--AZUREML_EXPERIMENT_ID", type=str, help="AZUREML_EXPERIMENT_ID")
parser.add_argument("--_AML_PARAMETER_BatchRunCorrelationId", type=str, help="_AML_PARAMETER_BatchRunCorrelationId")

args = parser.parse_args()

SOURCE_READ_SPN_VALUE = args.SOURCE_READ_SPN_VALUE
SOURCE_READ_SPNKEY_VALUE = args.SOURCE_READ_SPNKEY_VALUE
AML_STORAGE_SPN_VALUE = args.AML_STORAGE_SPN_VALUE
AML_STORAGE_SPNKEY_VALUE = args.AML_STORAGE_SPNKEY_VALUE
SOURCE_STORAGE_ACCOUNT_VALUE = args.SOURCE_STORAGE_ACCOUNT_VALUE
SOURCE_READ_PATH = args.SOURCE_READ_PATH
AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE = args.AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE
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
EXPERIMENT_NAME = args.EXPERIMENT_NAME
AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH = args.AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH
KUSTO_SOURCE_READ_SPN = args.KUSTO_SOURCE_READ_SPN
KUSTO_SOURCE_READ_SPNKEY = args.KUSTO_SOURCE_READ_SPNKEY
CX_GRAPH_LINEAGE_ENDPOINT = args.CX_GRAPH_LINEAGE_ENDPOINT
CX_GRAPH_LINEAGE_ENDPOINT_KEY = args.CX_GRAPH_LINEAGE_ENDPOINT_KEY


AZUREML_SCRIPT_DIRECTORY_NAME = args.AZUREML_SCRIPT_DIRECTORY_NAME
AZUREML_RUN_TOKEN = args.AZUREML_RUN_TOKEN
AZUREML_RUN_TOKEN_EXPIRY = args.AZUREML_RUN_TOKEN_EXPIRY
AZUREML_RUN_ID = args.AZUREML_RUN_ID
AZUREML_ARM_SUBSCRIPTION = args.AZUREML_ARM_SUBSCRIPTION
AZUREML_ARM_RESOURCEGROUP = args.AZUREML_ARM_RESOURCEGROUP
AZUREML_ARM_WORKSPACE_NAME = args.AZUREML_ARM_WORKSPACE_NAME
AZUREML_ARM_PROJECT_NAME = args.AZUREML_ARM_PROJECT_NAME
AZUREML_SERVICE_ENDPOINT = args.AZUREML_SERVICE_ENDPOINT
AZUREML_WORKSPACE_ID = args.AZUREML_WORKSPACE_ID
AZUREML_EXPERIMENT_ID = args.AZUREML_EXPERIMENT_ID

########### Read Parameters Passed From Previous Step
import os
secretscope = os.getenv("secretscope")

SOURCE_READ_SPN_VALUE = dbutils.secrets.get(secretscope,SOURCE_READ_SPN_VALUE)
SOURCE_READ_SPNKEY_VALUE = dbutils.secrets.get(secretscope,SOURCE_READ_SPNKEY_VALUE)
AML_STORAGE_SPN_VALUE = dbutils.secrets.get(secretscope,AML_STORAGE_SPN_VALUE)
AML_STORAGE_SPNKEY_VALUE = dbutils.secrets.get(secretscope,AML_STORAGE_SPNKEY_VALUE)
AML_LOGGER_APPINSIGHT_ID = dbutils.secrets.get(secretscope,AML_LOGGER_APPINSIGHT_ID)
AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY = dbutils.secrets.get(secretscope,AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY)
KUSTO_SOURCE_READ_SPN = dbutils.secrets.get(secretscope,KUSTO_SOURCE_READ_SPN)
KUSTO_SOURCE_READ_SPNKEY = dbutils.secrets.get(secretscope,KUSTO_SOURCE_READ_SPNKEY)
CX_GRAPH_LINEAGE_ENDPOINT = dbutils.secrets.get(secretscope,CX_GRAPH_LINEAGE_ENDPOINT)
CX_GRAPH_LINEAGE_ENDPOINT_KEY = dbutils.secrets.get(secretscope,CX_GRAPH_LINEAGE_ENDPOINT_KEY)

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
spark.conf.set("fs.azure.account.auth.type."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", SOURCE_READ_SPN_VALUE)
spark.conf.set("fs.azure.account.oauth2.client.secret."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", SOURCE_READ_SPNKEY_VALUE)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", "https://login.microsoftonline.com/9493c38c-a42d-44cc-baef-40b3ee092c52/oauth2/token")

dftemp=spark.read.format('parquet').load("abfss://dev@mlopssynapseadlsgen2.dfs.core.windows.net/run-temp")
row = dftemp.first()
RUN_ID,ADFPIPELINE,TRIGGERNAME,DATAFACTORYNAME = row["RUN_ID"],row["ADFPIPELINE"],row["TRIGGERNAME"],row["DATAFACTORYNAME"]


###Simply Adding the Logger and running in the Current Run Context
### ALL The files from CWD are uploaded to dbfs:/AZUREML_SCRIPT_DIRECTORY_NAME
with open('/dbfs/'+AZUREML_SCRIPT_DIRECTORY_NAME+'/core/app_insights_logger.py', 'r') as f:
    script = f.read()
exec(script)

with open('/dbfs/'+AZUREML_SCRIPT_DIRECTORY_NAME+'/core/lineagegraph.py', 'r') as f:
    script = f.read()
exec(script)

######## Add The Required Data Engineering Code Below

amlPipelinelogger= telemetrylogger(AML_LOGGER_APPINSIGHT_ID,\
                                    AML_LOGGER_APPINSIGHT_INSTRUMENTATION_KEY,\
                                    AML_SUBSCRIPTION_ID,\
                                    EXPERIMENT_NAME,\
                                    PIPELINE_ID)

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
documentInfo = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').values('id')")[0]

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


import os
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

###Simply Adding the Spark Utils in the Current Run Context

with open('/dbfs/'+AZUREML_SCRIPT_DIRECTORY_NAME+'/core/sparkutils.py', 'r') as f:
    script = f.read()
exec(script)

####################

cluster = 'https://mlopskustoeus01prod.westus.kusto.windows.net'
database = 'kustodb'
table = 'iris_data'
app_id = KUSTO_SOURCE_READ_SPN
app_key = KUSTO_SOURCE_READ_SPNKEY
tenant_id = '9493c38c-a42d-44cc-baef-40b3ee092c52'

kustoOptions = {"kustoCluster":cluster
                , "kustoDatabase" : database
                , "kustoTable" :table
                ,"kustoAADClientID":app_id
                ,"kustoClientAADClientPassword":app_key
                ,"kustoAADAuthorityID":tenant_id}

kustoDf  = read_from_kusto(kustoOptions,RUN_ID,PIPELINE_STEP_NAME)

write_to_adls_gen2(SOURCE_STORAGE_ACCOUNT_VALUE,AML_STORAGE_EXPERIMENT_KUSTO_ROOT_PATH,'parquet',1,kustoDf,SOURCE_READ_SPN_VALUE,SOURCE_READ_SPNKEY_VALUE,RUN_ID,PIPELINE_STEP_NAME)
RowCount = kustoDf.count()

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
                            'EXPERIMENT_NAME':EXPERIMENT_NAME,\
                            'START_TIME':str(StartTime),\
                            'END_TIME':str(EndTime),\
                            'STATUS':'COMPLETED',\
                            'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds())},{'DataRowCount':RowCount})

documentId = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'PreTrainDatabricksSparkKustoDataEngineeringStep').values('id')")[0]

LineageLogger.update_vertex(documentId, {'END_TIME':str(EndTime),\
                                    'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds()),\
                                    'DataRowCount':RowCount})

source_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'PreTrainSynapseSparkDataEngineeringStep').values('id')")[0]

target_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', '"+PIPELINE_STEP_NAME+"').values('id')")[0]

LineageLogger.insert_edges(source_v_id, target_v_id,"isFollowedBy", None)