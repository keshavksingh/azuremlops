###Simply Adding the Logger and running in the Current Run Context
with open('./core/app_insights_logger.py', 'r') as f:
    script = f.read()
exec(script)

###Simply Adding the Lineage and running in the Current Run Context
with open('./core/lineagegraph.py', 'r') as f:
    script = f.read()
exec(script)

############### Add great_expectations Package For Data Quality
#https://greatexpectations.io/expectations/
#https://docs.greatexpectations.io/docs/reference/expectations/result_format/
import subprocess
import pkg_resources

package_name = ["great_expectations"]
for pkg in package_name:
  try:
    pkg_resources.get_distribution(pkg)
    print(f"{pkg} is already installed.")
  except pkg_resources.DistributionNotFound:
    print(f"{pkg} is not installed.")
    try:
      subprocess.check_call(["pip", "install", pkg])
      print(f"Successfully installed {pkg}!")
    except subprocess.CalledProcessError:
      print(f"Failed to install {pkg}.")

#### Add Data Engineering Script Below
import argparse
import datetime
parser = argparse.ArgumentParser()

parser.add_argument("--SOURCE_READ_SPN_VALUE", type=str, help="SOURCE_READ_SPN_VALUE")
parser.add_argument("--SOURCE_READ_SPNKEY_VALUE", type=str, help="SOURCE_READ_SPNKEY_VALUE")
parser.add_argument("--SOURCE_STORAGE_ACCOUNT_VALUE", type=str, help="SOURCE_STORAGE_ACCOUNT_VALUE")
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
SOURCE_STORAGE_ACCOUNT_VALUE = args.SOURCE_STORAGE_ACCOUNT_VALUE
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
import great_expectations as ge
sc = SparkContext.getOrCreate();

spark = SparkSession(sc)

from pyspark.sql import SparkSession
###Simply Adding the Spark Utils in the Current Run Context
with open('./core/sparkutils.py', 'r') as f:
    script = f.read()
exec(script)

############# Read Data
spark_df = read_from_adls_gen2(SOURCE_STORAGE_ACCOUNT_VALUE,\
                         AML_STORAGE_EXPERIMENT_ROOT_PATH_VALUE,\
                         'parquet',\
                         SOURCE_READ_SPN_VALUE,\
                         SOURCE_READ_SPNKEY_VALUE,\
                         RUN_ID,\
                         PIPELINE_STEP_NAME)

dfspark_ge = ge.dataset.SparkDFDataset(spark_df)
### Great Expectation Spark DataFrame Example
dqresult = dfspark_ge.expect_column_to_exist("sepalLenght",result_format={"result_format": "COMPLETE"})

CurrTime = datetime.datetime.now()
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
                            'DQ_RESULT':str(dqresult),\
                            'CURRENT_TIME':str(CurrTime)},None)

### Great Expectations Pandas DataFrame Example
pandas_df = spark_df.toPandas()
dfpandas_ge = ge.dataset.PandasDataset(pandas_df)
dqresult = dfpandas_ge.expect_column_to_exist("sepalWidth",result_format={"result_format": "COMPLETE"})

CurrTime = datetime.datetime.now()
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
                            'DQ_RESULT':str(dqresult),\
                            'CURRENT_TIME':str(CurrTime)},None)

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
                            'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds())},None)

documentId = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'PreTrainDatabricksSparkKustoDataEngineeringStep').values('id')")[0]

LineageLogger.update_vertex(documentId, {'END_TIME':str(EndTime),\
                                    'DURATION_IN_SEC':str((EndTime - StartTime).total_seconds())})

source_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', 'PreTrainSqlDataEngineeringStep').values('id')")[0]

target_v_id = LineageLogger.query_graph("g.V().hasLabel('amlrun').has('RUN_ID', '"+RUN_ID+"').has('PIPELINE_STEP_NAME', '"+PIPELINE_STEP_NAME+"').values('id')")[0]

LineageLogger.insert_edges(source_v_id, target_v_id,"isFollowedBy", None)