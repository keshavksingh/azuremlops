import os 
import sys 
values = sys.argv[1]
split_values = values.split(',')

platform_client_id,platform_client_secret,projectroot,environment = split_values[0],split_values[1],split_values[2],split_values[3]

artifacts_dir = os.path.join('.', '_azuremlops',projectroot)
#artifacts_dir = "./template-project"

###Simply Adding the Util and running in the Current Run Context
with open(artifacts_dir+"/core/util.py", 'r') as f:
    script = f.read()
exec(script)

package_name = ["pyyaml","azure-identity","azure-keyvault-secrets","azureml-core", "pywin32","applicationinsights","azureml-pipeline","azureml-pipeline-steps","azure-keyvault","azure-mgmt-datafactory"]
install_pip(package_name)


#Get Set Environment
env_yaml = environment #get_environment(artifacts_dir+"/core/ENV.yaml")
print(env_yaml)
set_environment_params,set_environment_var=set_environment(env_yaml)
print("Picking Params from Section- "+set_environment_params+" And Picking Environmnet Variable from Section - "+set_environment_var+" Of setup.ini")

############ Get DEPLOYMENT TARGET RESOURCE CONFIGURATION

import os
import json
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

with open(artifacts_dir+"/resourceconfig.json", 'r') as config_file:
    config = json.load(config_file)

sourcekeyvault = config[env_yaml]["sourcekeyvault"]
target_keyvault = config[env_yaml]["target_keyvault"]
target_subscriptionid = config[env_yaml]["target_subscriptionid"]
target_resourcegroup = config[env_yaml]["target_resourcegroup"]
target_amlws = config[env_yaml]["target_amlws"]
target_data_factory_name = config[env_yaml]["target_data_factory_name"]
target_amlwsrootdirectory = config[env_yaml]["target_amlwsrootdirectory"]
target_aml_pipeline_name = config[env_yaml]["target_aml_pipeline_name"]
target_tenantid = config[env_yaml]["target_tenantid"]
target_adf_pipeline_name = config[env_yaml]["target_adf_pipeline_name"]
target_trigger_name = config[env_yaml]["target_trigger_name"]
target_triger_interval_hour = config[env_yaml]["target_triger_interval_hour"]

###################### Azure Data Factory AML Linked Service
print("BEGIN ROLLBACK!")
import json
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.mgmt.datafactory.models import LinkedServiceResource, SecureString
from azure.core.exceptions import HttpResponseError
credential = ClientSecretCredential(target_tenantid, platform_client_id, platform_client_secret)
# Authenticate with Azure
adf_client = DataFactoryManagementClient(credential, target_subscriptionid)

####################### Azure Data Factory STOP TRIGGER IF EXISTS & DELETE TRIGGER
print("STOP Azure Data Factory TRIGGER IF EXISTS & DELETE THE TRIGGER!")
try:
    adf_client.triggers.get(target_resourcegroup, target_data_factory_name, target_trigger_name)
    print(f"Trigger {target_trigger_name} already exists.")
    response = adf_client.triggers.begin_stop(
        resource_group_name=target_resourcegroup,
        factory_name=target_data_factory_name,
        trigger_name=target_trigger_name).result()
    print(f"{target_trigger_name} trigger stopped successfully.")
    adf_client.triggers.delete(
        resource_group_name=target_resourcegroup,
        factory_name=target_data_factory_name,
        trigger_name=target_trigger_name)
    print(f"{target_trigger_name} trigger STOPPED & DELETED successfully.")
except HttpResponseError as ex:
    if 'NotFound' in str(ex):
        print(f"Trigger {target_trigger_name} does not exist!")
print("Rollback COMPLETED!")