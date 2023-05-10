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

################################### Deploy Key Vault Secrets
print(f"Copying Key Vault Secrets from {sourcekeyvault}  to {target_keyvault}")
credential = ClientSecretCredential(target_tenantid, platform_client_id, platform_client_secret)
target_kv_client = SecretClient(vault_url=f"https://{target_keyvault}.vault.azure.net", credential=credential)

# Delete all secrets in targetkeyvault
secrets = target_kv_client.list_properties_of_secrets()
for secret in secrets:
    deleted_secret = target_kv_client.begin_delete_secret(secret.name)
    secret = deleted_secret.result()
    deleted_secret.wait()
    purge_deleted = target_kv_client.purge_deleted_secret(secret.name)

import time
time.sleep(45)

# Copy all secrets from sourcekeyvault to targetkeyvault
source_client = SecretClient(vault_url=f"https://{sourcekeyvault}.vault.azure.net", credential=credential)
source_secrets = source_client.list_properties_of_secrets()
for source_secret in source_secrets:
    secret = source_client.get_secret(source_secret.name)
    target_kv_client.set_secret(source_secret.name, secret.value)

print(f"COMPLETED Copying Key Vault Secrets from {sourcekeyvault}  to {target_keyvault}")
############################################## PUBLISH AML PIPELINE
print("BEGIN Publishing AML Pipeline.")

###Simply Adding the Util and running in the Current Run Context
with open(artifacts_dir+"/publishPipeline.py", 'r') as f:
    script = f.read()
exec(script)

print("Publishing Pipeline Complete!")
#################### Get the AML PipelineId
print("Get Published Pipeline Id")
from azureml.pipeline.core import PublishedPipeline

All_PublishedPipeline = PublishedPipeline.list(workspace=workspace, active_only=True)
matching_endpoints = [ep for ep in PublishedPipeline.list(workspace=workspace, active_only=True) if ep.name == target_aml_pipeline_name]
print(matching_endpoints)
if len(matching_endpoints) == 0:
    print(f"No published endpoints found with name '{target_aml_pipeline_name}'")
    latest_aml_pl_id=None
else:
    # Sort endpoints by creation time and retrieve latest
    latest_endpoint = matching_endpoints[0]
    latest_aml_pl_id = latest_endpoint.id
    print(f"Latest published endpoint ID for '{latest_endpoint.name}': {latest_endpoint.id}")
###################### Azure Data Factory AML Linked Service
print("START ADF AML LINKED SERVICE PUBLISH")
import json
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.mgmt.datafactory.models import LinkedServiceResource, SecureString
from azure.core.exceptions import HttpResponseError

credential = ClientSecretCredential(target_tenantid, platform_client_id, platform_client_secret)

# Authenticate with Azure
adf_client = DataFactoryManagementClient(credential, target_subscriptionid)
########## Create AML Linked Service if not Exists
linked_service_name="LS_AzureMLService"
try:
    adf_client.linked_services.get(target_resourcegroup, target_data_factory_name, linked_service_name)
    print(f"Linked service {linked_service_name} already exists.")
except HttpResponseError as ex:
    if 'NotFound' in str(ex):
        print(f"Linked service {linked_service_name} does not exist. Creating it...")
        response = adf_client.linked_services.create_or_update(
        resource_group_name=target_resourcegroup,
        factory_name=target_data_factory_name,
        linked_service_name="LS_AzureMLService",
        linked_service=LinkedServiceResource(properties={
                "annotations": [],
                "type": "AzureMLService",
                "typeProperties": {
                                    "subscriptionId": target_subscriptionid,
                                    "resourceGroupName": target_resourcegroup,
                                    "mlWorkspaceName": target_amlws,
                                    "tenant": target_tenantid,
                                    "servicePrincipalId": platform_client_id,
                                    "servicePrincipalKey": SecureString(value=platform_client_secret)
                                }
                    }))
        print(f"{linked_service_name} linked service created successfully.")
    else:
        raise ex
print("COMPLETE ADF AML LINKED SERVICE PUBLISH")
####################### Azure Data Factory CREATE Pipeline IF NOT EXISTS
print("CREATE ADF MASTER PIPELINE")
try:
    adf_client.pipelines.get(target_resourcegroup, target_data_factory_name, target_adf_pipeline_name)
    print(f"Pipeline {target_adf_pipeline_name} already exists.")
except HttpResponseError as ex:
    if 'NotFound' in str(ex):
        print(f"Pipeline {target_adf_pipeline_name} does not exist. Creating it...")
        response = adf_client.pipelines.create_or_update(
        resource_group_name=target_resourcegroup,
        factory_name=target_data_factory_name,
        pipeline_name=target_adf_pipeline_name,
        pipeline={"properties": {
                "activities": [
                    {
                        "name": "Machine Learning Execute Pipeline",
                        "type": "AzureMLExecutePipeline",
                        "dependsOn": [],
                        "policy": {
                            "timeout": "0.12:00:00",
                            "retry": 0,
                            "retryIntervalInSeconds": 30,
                            "secureOutput": False,
                            "secureInput": False
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "mlPipelineParameters": {
                                "BatchRunCorrelationId": {
                                    "value": "@pipeline().RunId",
                                    "type": "Expression"
                                },
                                "ADFPipeline": {
                                    "value": "@pipeline().Pipeline",
                                    "type": "Expression"
                                },
                                "TriggerName": {
                                    "value": "@pipeline().TriggerName",
                                    "type": "Expression"
                                },
                                "DatafactoryName": {
                                    "value": "@pipeline().DataFactory",
                                    "type": "Expression"
                                }
                            },
                            "mlExecutionType": "pipeline",
                            "mlPipelineId": {
                                "value": "@pipeline().parameters.AMLPipelineId",
                                "type": "Expression"
                            }
                        },
                        "linkedServiceName": {
                            "referenceName": linked_service_name,
                            "type": "LinkedServiceReference"
                        }
                    }
                ],
                "parameters": {
                    "AMLPipelineId": {
                        "type": "string",
                        "defaultValue": latest_aml_pl_id
                    }
                }
            }},
        )
        print(f"{target_adf_pipeline_name} pipeline created successfully.")
    else:
        raise ex
print("COMPLETE CREATING ADF MASTER PIPELINE")
####################### Azure Data Factory CREATE TRIGGER IF NOT EXISTS & START TRIGGER
print("Azure Data Factory CREATE TRIGGER IF NOT EXISTS & SCHEDULE START TRIGGER")
import datetime
current_time = (datetime.datetime.utcnow()+datetime.timedelta(hours=-(target_triger_interval_hour+2))).strftime('%Y-%m-%dT%H:%M:%SZ')
print(str(current_time))
try:
    adf_client.triggers.get(target_resourcegroup, target_data_factory_name, target_trigger_name)
    print(f"Trigger {target_trigger_name} already exists.")
except HttpResponseError as ex:
    if 'NotFound' in str(ex):
        print(f"Trigger {target_trigger_name} does not exist. Creating it...")
        response = adf_client.triggers.create_or_update(
        resource_group_name=target_resourcegroup,
        factory_name=target_data_factory_name,
        trigger_name=target_trigger_name,
        trigger={"properties": {
                "annotations": [],
                "runtimeState": "Started",
                "pipeline": {
                                "pipelineReference": {
                                    "referenceName": target_adf_pipeline_name,
                                    "type": "PipelineReference"
                                },
                                "parameters": {
                                    "AMLPipelineId": latest_aml_pl_id
                                }
                            },
                "type": "TumblingWindowTrigger",
                "typeProperties": {
                                    "frequency": "Hour",
                                    "interval": target_triger_interval_hour,
                                    "startTime": str(current_time),
                                    "delay": "00:00:00",
                                    "maxConcurrency": 1,
                                    "retryPolicy": {
                                        "intervalInSeconds": 180
                                    },
                                    "dependsOn": []}
            }}
        )
        print(f"{target_trigger_name} trigger created successfully.")

        response = adf_client.triggers.begin_start(
        resource_group_name=target_resourcegroup,
        factory_name=target_data_factory_name,
        trigger_name=target_trigger_name).result()
        print(f"{target_trigger_name} trigger scheduled & started successfully.")
        
    else:
        raise ex
print("COMPLETED Azure Data Factory CREATE TRIGGER IF NOT EXISTS & SCHEDULE START TRIGGER")
print("DEPLOYMENT COMPLETED!")