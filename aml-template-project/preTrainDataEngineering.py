###Simply Adding the Logger and running in the Current Run Context
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
parser.add_argument("--ADFPIPELINE", type=str, help="ADFPIPELINE")
parser.add_argument("--TRIGGERNAME", type=str, help="TRIGGERNAME")
parser.add_argument("--DATAFACTORYNAME", type=str, help="DATAFACTORYNAME")
parser.add_argument("--EXPERIMENT_NAME", type=str, help="EXPERIMENT_NAME")
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT")
parser.add_argument("--CX_GRAPH_LINEAGE_ENDPOINT_KEY", type=str, help="CX_GRAPH_LINEAGE_ENDPOINT_KEY")

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



from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate();

spark = SparkSession(sc)

from pyspark.sql import SparkSession

###Simply Adding the Spark Utils in the Current Run Context
with open('./core/sparkutils.py', 'r') as f:
    script = f.read()
exec(script)

############# Read Data
df = read_from_adls_gen2(SOURCE_STORAGE_ACCOUNT_VALUE,\
                         SOURCE_READ_PATH,\
                         'delta',\
                         SOURCE_READ_SPN_VALUE,\
                         SOURCE_READ_SPNKEY_VALUE,\
                         RUN_ID,\
                         PIPELINE_STEP_NAME)
############# Write Data
write_to_adls_gen2(SOURCE_STORAGE_ACCOUNT_VALUE,\
                   AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE,\
                   'parquet',\
                   1,\
                   df,\
                   SOURCE_READ_SPN_VALUE,\
                   SOURCE_READ_SPNKEY_VALUE,\
                   RUN_ID,\
                   PIPELINE_STEP_NAME)

RowCount = df.count()

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
                            'START_TIME':str(StartTime),\
                            'END_TIME':str(EndTime),\
                            'STATUS':'COMPLETED',\
                            'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds())},{'DataRowCount':RowCount})

documentId = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', '"+PIPELINE_STEP_NAME+"').values('id')")[0]
LineageLogger.update_vertex(documentId, {'END_TIME':str(EndTime),\
                                    'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds()),\
                                    'DataRowCount':RowCount})

# Add the parameters and write to the output file
dftemp = spark.sql("SELECT '"+str(RUN_ID)+"' AS RUN_ID, '"+str(ADFPIPELINE)+"' AS ADFPIPELINE,'"+str(TRIGGERNAME)+"' AS TRIGGERNAME,'"+str(DATAFACTORYNAME)+"' AS DATAFACTORYNAME")

spark = SparkSession.builder.appName("Write To ADLS Gen2").getOrCreate()
spark.conf.set("fs.azure.account.auth.type."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", SOURCE_READ_SPN_VALUE)
spark.conf.set("fs.azure.account.oauth2.client.secret."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", SOURCE_READ_SPNKEY_VALUE)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+SOURCE_STORAGE_ACCOUNT_VALUE+".dfs.core.windows.net", "https://login.microsoftonline.com/9493c38c-a42d-44cc-baef-40b3ee092c52/oauth2/token")

dftemp.write.format('parquet').mode('overwrite').save("abfss://dev@mlopssynapseadlsgen2.dfs.core.windows.net/run-temp")
