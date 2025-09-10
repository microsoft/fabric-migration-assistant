import requests
import json
import os
from datetime import datetime
from collections import defaultdict
from azure.identity import DefaultAzureCredential
from azure.mgmt.synapse import SynapseManagementClient

def load_config(config_file="config.json"):
    """Load configuration from JSON file."""
    if not os.path.exists(config_file):
        # Create a sample config file if it doesn't exist
        sample_config = {
            "subscription_ids": [
                "<your-subscription-id-here>"
            ]
        }
        with open(config_file, 'w') as f:
            json.dump(sample_config, f, indent=2)
        print(f"📁 Created sample config file: {config_file}")
        print("   Please update it with your subscription IDs and run again.")
        return sample_config
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Validate config structure
        if 'subscription_ids' not in config:
            raise ValueError("Config file must contain 'subscription_ids' key")
        
        if not isinstance(config['subscription_ids'], list):
            raise ValueError("'subscription_ids' must be a list")
        
        if len(config['subscription_ids']) == 0:
            raise ValueError("At least one subscription ID must be provided")
        
        return config
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in config file: {e}")
        raise
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        raise

def list_workspaces(subscription_id):
    """List all Synapse workspaces in a subscription."""
    client = SynapseManagementClient(
        credential=DefaultAzureCredential(),
        subscription_id=subscription_id,
    )
    return list(client.workspaces.list())

def list_notebooks(subscription_id, workspace_name, resource_group, credential, api_version="2020-12-01"):
    """List notebooks in a Synapse workspace via REST API (Data Plane)."""
    token = credential.get_token("https://dev.azuresynapse.net/.default").token
    dev_endpoint = f"https://{workspace_name}.dev.azuresynapse.net"
    url = f"{dev_endpoint}/notebooks?api-version={api_version}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch notebooks for workspace {workspace_name}: {response.text}")
        return []
    return response.json().get("value", [])

def list_big_data_pools(subscription_id, resource_group, workspace_name, credential, api_version="2021-06-01"):
    """List all Big Data (Spark) pools in a Synapse workspace."""
    token = credential.get_token("https://management.azure.com/.default").token
    mgmt_endpoint = "https://management.azure.com"
    url = f"{mgmt_endpoint}/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Synapse/workspaces/{workspace_name}/bigDataPools?api-version={api_version}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch Big Data pools for workspace {workspace_name}: {response.text}")
        return []
    return response.json().get("value", [])

def list_spark_job_definitions(workspace_name, credential, api_version="2020-12-01"):
    """List all Spark Job Definitions in a Synapse workspace (Data Plane)."""
    token = credential.get_token("https://dev.azuresynapse.net/.default").token
    dev_endpoint = f"https://{workspace_name}.dev.azuresynapse.net"
    url = f"{dev_endpoint}/sparkJobDefinitions?api-version={api_version}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch Spark Job Definitions for workspace {workspace_name}: {response.text}")
        return []
    return response.json().get("value", [])

def parse_resource_group_from_id(resource_id: str) -> str:
    """Extract resource group from workspace ID."""
    parts = resource_id.split("/")
    try:
        rg_index = parts.index("resourceGroups") + 1
        return parts[rg_index]
    except (ValueError, IndexError):
        return None

def extract_workspace_details(ws):
    """Extract workspace details for JSON export."""
    props = ws.as_dict().get("properties", {})
    
    # Extract private endpoint connections
    pe_connections = []
    for pe in props.get("privateEndpointConnections", []):
        pe_props = pe.get("properties", {})
        pe_endpoint = pe_props.get("privateEndpoint", {})
        pe_link_state = pe_props.get("privateLinkServiceConnectionState", {})
        
        pe_connections.append({
            "name": pe.get("name"),
            "id": pe.get("id"),
            "type": pe.get("type"),
            "provisioning_state": pe_props.get("provisioningState"),
            "private_endpoint_id": pe_endpoint.get("id"),
            "link_status": pe_link_state.get("status"),
            "actions_required": pe_link_state.get("actionsRequired"),
            "description": pe_link_state.get("description")
        })
    
    # Extract managed virtual network settings
    mvs = props.get("managedVirtualNetworkSettings", {})
    managed_vnet_settings = {
        "allowed_aad_tenants": mvs.get("allowedAadTenantIdsForLinking", []),
        "linked_access_check": mvs.get("linkedAccessCheckOnTargetResource", False),
        "prevent_data_exfiltration": mvs.get("preventDataExfiltration", False)
    }
    
    return {
        "name": ws.name,
        "id": ws.id,
        "location": ws.location,
        "provisioning_state": props.get("provisioningState"),
        "managed_resource_group": props.get("managedResourceGroupName"),
        "workspace_uid": props.get("workspaceUID"),
        "private_endpoint_connections": pe_connections,
        "managed_vnet_settings": managed_vnet_settings
    }

def extract_notebook_details(notebook):
    """Extract notebook details for JSON export."""
    props = notebook.get("properties", {})
    pool = props.get("bigDataPool", {})
    session = props.get("sessionProperties", {})
    
    return {
        "name": notebook.get("name"),
        "description": props.get("description"),
        "format": f"v{props.get('nbformat')}.{props.get('nbformat_minor')}",
        "big_data_pool": {
            "reference_name": pool.get("referenceName"),
            "type": pool.get("type")
        } if pool else None,
        "session_properties": {
            "driver_memory": session.get("driverMemory"),
            "driver_cores": session.get("driverCores"),
            "executor_memory": session.get("executorMemory"),
            "executor_cores": session.get("executorCores"),
            "num_executors": session.get("numExecutors")
        } if session else None
    }

def extract_big_data_pool_details(pool):
    """Extract Big Data pool details for JSON export."""
    props = pool.get("properties", {})
    auto_scale = props.get("autoScale", {})
    auto_pause = props.get("autoPause", {})
    dyn_alloc = props.get("dynamicExecutorAllocation", {})
    lib_req = props.get("libraryRequirements", {})
    
    return {
        "name": pool.get("name"),
        "spark_version": props.get("sparkVersion"),
        "provisioning_state": props.get("provisioningState"),
        "node_count": props.get("nodeCount"),
        "node_size": props.get("nodeSize"),
        "node_size_family": props.get("nodeSizeFamily"),
        "auto_scale": {
            "enabled": auto_scale.get("enabled"),
            "min_node_count": auto_scale.get("minNodeCount"),
            "max_node_count": auto_scale.get("maxNodeCount")
        } if auto_scale else None,
        "auto_pause": {
            "enabled": auto_pause.get("enabled"),
            "delay_minutes": auto_pause.get("delayInMinutes")
        } if auto_pause else None,
        "dynamic_executor_allocation": {
            "enabled": dyn_alloc.get("enabled"),
            "min_executors": dyn_alloc.get("minExecutors"),
            "max_executors": dyn_alloc.get("maxExecutors")
        } if dyn_alloc else None,
        "library_requirements": {
            "filename": lib_req.get("filename"),
            "last_updated": lib_req.get("time")
        } if lib_req else None,
        "custom_libraries_count": len(props.get("customLibraries", []))
    }

def extract_spark_job_definition_details(job_def):
    """Extract Spark Job Definition details for JSON export."""
    props = job_def.get("properties", {})
    pool = props.get("targetBigDataPool", {})
    folder = props.get("folder", {})
    job_props = props.get("jobProperties", {})
    
    return {
        "name": job_def.get("name"),
        "id": job_def.get("id"),
        "type": job_def.get("type"),
        "etag": job_def.get("etag"),
        "description": props.get("description"),
        "required_spark_version": props.get("requiredSparkVersion"),
        "target_big_data_pool": {
            "reference_name": pool.get("referenceName"),
            "type": pool.get("type")
        } if pool else None,
        "folder": folder.get("name") if folder else None,
        "job_properties": {
            "name": job_props.get("name"),
            "file": job_props.get("file"),
            "class_name": job_props.get("className"),
            "language": props.get("language"),
            "driver_memory": job_props.get("driverMemory"),
            "driver_cores": job_props.get("driverCores"),
            "executor_memory": job_props.get("executorMemory"),
            "executor_cores": job_props.get("executorCores"),
            "num_executors": job_props.get("numExecutors"),
            "args": job_props.get("args", []),
            "jars": job_props.get("jars", []),
            "py_files": job_props.get("pyFiles", []),
            "files": job_props.get("files", []),
            "archives": job_props.get("archives", [])
        } if job_props else None
    }

def print_workspace_details(ws):
    """Print detailed Synapse workspace properties, including private endpoints."""
    props = ws.as_dict().get("properties", {})
    print(f"📂 Workspace: {ws.name}")
    print(f"   • ID: {ws.id}")
    print(f"   • Location: {ws.location}")
    print(f"   • Provisioning State: {props.get('provisioningState')}")
    print(f"   • Managed Resource Group: {props.get('managedResourceGroupName')}")
    print(f"   • Workspace UID: {props.get('workspaceUID')}")
    
    # Print private endpoint connections
    pe_connections = props.get("privateEndpointConnections", [])
    if pe_connections:
        print(f"   • Private Endpoint Connections: {len(pe_connections)}")
        for pe in pe_connections:
            pe_props = pe.get("properties", {})
            pe_endpoint = pe_props.get("privateEndpoint", {})
            pe_link_state = pe_props.get("privateLinkServiceConnectionState", {})
            print(f"      🔒 Private Endpoint Connection: {pe.get('name')}")
            print(f"         • ID: {pe.get('id')}")
            print(f"         • Type: {pe.get('type')}")
            print(f"         • Provisioning State: {pe_props.get('provisioningState')}")
            print(f"         • Private Endpoint ID: {pe_endpoint.get('id', 'N/A')}")
            print(f"         • Link Status: {pe_link_state.get('status')}")
            print(f"         • Actions Required: {pe_link_state.get('actionsRequired', 'None')}")
            print(f"         • Description: {pe_link_state.get('description', 'N/A')}")
    else:
        print("   • Private Endpoint Connections: None")
    
    # Managed Virtual Network settings (if present)
    mvs = props.get("managedVirtualNetworkSettings", {})
    print("   • Managed VNet Settings:")
    print(f"      - Allowed AAD Tenants: {mvs.get('allowedAadTenantIdsForLinking', [])}")
    print(f"      - Linked Access Check: {mvs.get('linkedAccessCheckOnTargetResource', False)}")
    print(f"      - Prevent Data Exfiltration: {mvs.get('preventDataExfiltration', False)}")
    print("")

def print_notebook_details(notebook):
    """Print detailed notebook properties nicely."""
    name = notebook.get("name")
    props = notebook.get("properties", {})
    print(f"   📝 Notebook: {name}")
    print(f"      • Description: {props.get('description', 'N/A')}")
    print(f"      • Format: v{props.get('nbformat')}.{props.get('nbformat_minor')}")
    pool = props.get("bigDataPool")
    if pool:
        print(f"      • BigDataPool: {pool.get('referenceName')} (Type: {pool.get('type')})")
    session = props.get("sessionProperties", {})
    if session:
        print("      • Session Properties:")
        print(f"         - Driver Memory: {session.get('driverMemory')}")
        print(f"         - Driver Cores: {session.get('driverCores')}")
        print(f"         - Executor Memory: {session.get('executorMemory')}")
        print(f"         - Executor Cores: {session.get('executorCores')}")
        print(f"         - Num Executors: {session.get('numExecutors')}")
    print("")

def print_big_data_pool_details(pool):
    """Print detailed Spark Big Data pool properties."""
    props = pool.get("properties", {})
    print(f"   🔥 Big Data Pool: {pool.get('name')}")
    print(f"      • Spark Version: {props.get('sparkVersion')}")
    print(f"      • Provisioning State: {props.get('provisioningState')}")
    print(f"      • Node Count: {props.get('nodeCount')}")
    print(f"      • Node Size: {props.get('nodeSize')} ({props.get('nodeSizeFamily')})")
    auto_scale = props.get("autoScale")
    if auto_scale:
        print(f"      • AutoScale: Enabled={auto_scale.get('enabled')}, Min={auto_scale.get('minNodeCount')}, Max={auto_scale.get('maxNodeCount')}")
    auto_pause = props.get("autoPause")
    if auto_pause:
        print(f"      • AutoPause: Enabled={auto_pause.get('enabled')}, Delay={auto_pause.get('delayInMinutes')} min")
    dyn_alloc = props.get("dynamicExecutorAllocation")
    if dyn_alloc:
        print(f"      • Dynamic Executor Allocation: Enabled={dyn_alloc.get('enabled')}, Min={dyn_alloc.get('minExecutors')}, Max={dyn_alloc.get('maxExecutors')}")
    if props.get("libraryRequirements"):
        lib_req = props["libraryRequirements"]
        print(f"      • Library Requirements File: {lib_req.get('filename')} (Last Updated: {lib_req.get('time')})")
    if props.get("customLibraries"):
        print(f"      • Custom Libraries: {len(props['customLibraries'])} uploaded")
    print("")

def print_spark_job_definition_details(job_def):
    """Print detailed Spark Job Definition properties."""
    print(f"   🚀 Spark Job Definition: {job_def.get('name')}")
    print(f"      • ID: {job_def.get('id')}")
    print(f"      • Type: {job_def.get('type')}")
    print(f"      • ETag: {job_def.get('etag')}")
    props = job_def.get("properties", {})
    print(f"      • Description: {props.get('description', 'N/A')}")
    print(f"      • Required Spark Version: {props.get('requiredSparkVersion')}")
    
    pool = props.get("targetBigDataPool")
    if pool:
        print(f"      • Target Big Data Pool: {pool.get('referenceName')} (Type: {pool.get('type')})")
    folder = props.get("folder")
    if folder:
        print(f"      • Folder: {folder.get('name')}")
    job_props = props.get("jobProperties", {})
    if job_props:
        print("      • Job Properties:")
        print(f"         - Name: {job_props.get('name')}")
        print(f"         - File: {job_props.get('file')}")
        print(f"         - ClassName: {job_props.get('className')}")
        print(f"         - Language: {props.get('language')}")
        print(f"         - Driver Memory: {job_props.get('driverMemory')}")
        print(f"         - Driver Cores: {job_props.get('driverCores')}")
        print(f"         - Executor Memory: {job_props.get('executorMemory')}")
        print(f"         - Executor Cores: {job_props.get('executorCores')}")
        print(f"         - Num Executors: {job_props.get('numExecutors')}")
        print(f"         - Args: {job_props.get('args', [])}")
        print(f"         - Jars: {job_props.get('jars', [])}")
        print(f"         - PyFiles: {job_props.get('pyFiles', [])}")
        print(f"         - Files: {job_props.get('files', [])}")
        print(f"         - Archives: {job_props.get('archives', [])}")
    print("")

def summarize_subscription(subscription_id, credential):
    """Summarize workspaces, job defs, notebooks, and spark pools."""
    workspaces = list_workspaces(subscription_id)
    print(f"\n📊 Subscription {subscription_id} Summary")
    print(f"   Total Workspaces: {len(workspaces)}\n")
    
    for ws in workspaces:
        resource_group = parse_resource_group_from_id(ws.id)
        notebooks = list_notebooks(subscription_id, ws.name, resource_group, credential)
        pools = list_big_data_pools(subscription_id, resource_group, ws.name, credential)
        jobs = list_spark_job_definitions(ws.name, credential)
        
        # Count notebooks by Spark version
        nb_by_runtime = defaultdict(int)
        for nb in notebooks:
            props = nb.get("properties", {})
            pool = props.get("bigDataPool", {})
            spark_version = "No pool attached"  # clearer label than "Unknown"
            # Find the pool's spark version (need to cross-check with pools list)
            if pool and "referenceName" in pool:
                pool_name = pool["referenceName"]
                matching_pool = next((p for p in pools if p.get("name") == pool_name), None)
                if matching_pool:
                    spark_version = matching_pool.get("properties", {}).get("sparkVersion", "Unknown")
            nb_by_runtime[spark_version] += 1
        
        # Count jobs by runtime
        jobs_by_runtime = defaultdict(int)
        for job in jobs:
            runtime = job.get("properties", {}).get("requiredSparkVersion", "Unknown")
            jobs_by_runtime[runtime] += 1
        
        print(f"📂 Workspace: {ws.name}")
        print(f"   • Spark Pools: {len(pools)}")
        print(f"   • Notebooks: {len(notebooks)} (by runtime: {dict(nb_by_runtime)})")
        print(f"   • Spark Job Definitions: {len(jobs)} (by runtime: {dict(jobs_by_runtime)})\n")

def write_to_json(subscription_id, credential, filename=None):
    """Write all workspace details to JSON file for a single subscription."""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"synapse_metadata_summary.json"
    
    workspaces = list_workspaces(subscription_id)
    
    # Build the complete JSON structure
    json_data = {
        "subscription_id": subscription_id,
        "scan_timestamp": datetime.now().isoformat(),
        "total_workspaces": len(workspaces),
        "workspaces": []
    }
    
    for ws in workspaces:
        resource_group = parse_resource_group_from_id(ws.id)
        
        # Get all workspace components
        notebooks = list_notebooks(subscription_id, ws.name, resource_group, credential)
        pools = list_big_data_pools(subscription_id, resource_group, ws.name, credential)
        jobs = list_spark_job_definitions(ws.name, credential)
        
        # Count notebooks by Spark version
        nb_by_runtime = defaultdict(int)
        for nb in notebooks:
            props = nb.get("properties", {})
            pool = props.get("bigDataPool", {})
            spark_version = "No pool attached"
            if pool and "referenceName" in pool:
                pool_name = pool["referenceName"]
                matching_pool = next((p for p in pools if p.get("name") == pool_name), None)
                if matching_pool:
                    spark_version = matching_pool.get("properties", {}).get("sparkVersion", "Unknown")
            nb_by_runtime[spark_version] += 1
        
        # Count jobs by runtime
        jobs_by_runtime = defaultdict(int)
        for job in jobs:
            runtime = job.get("properties", {}).get("requiredSparkVersion", "Unknown")
            jobs_by_runtime[runtime] += 1
        
        # Build workspace data structure
        workspace_data = {
            "workspace_details": extract_workspace_details(ws),
            "resource_group": resource_group,
            "notebooks": {
                "count": len(notebooks),
                "by_runtime": dict(nb_by_runtime),
                "details": [extract_notebook_details(nb) for nb in notebooks]
            },
            "big_data_pools": {
                "count": len(pools),
                "details": [extract_big_data_pool_details(pool) for pool in pools]
            },
            "spark_job_definitions": {
                "count": len(jobs),
                "by_runtime": dict(jobs_by_runtime),
                "details": [extract_spark_job_definition_details(job) for job in jobs]
            },
            "summary": {
                "spark_pools": len(pools),
                "notebooks": len(notebooks),
                "notebooks_by_runtime": dict(nb_by_runtime),
                "spark_job_definitions": len(jobs),
                "jobs_by_runtime": dict(jobs_by_runtime)
            }
        }
        
        json_data["workspaces"].append(workspace_data)
    
    # Write to JSON file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    
    print(f"📄 JSON export completed: {filename}")
    return filename

def write_consolidated_json(subscription_ids, credential, filename=None):
    """Write consolidated data for all subscriptions to a single JSON file."""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"synapse_workspaces_consolidated_{timestamp}.json"
    
    # Build consolidated JSON structure
    consolidated_data = {
        "scan_timestamp": datetime.now().isoformat(),
        "total_subscriptions": len(subscription_ids),
        "subscriptions": []
    }
    
    total_workspaces_across_subs = 0
    
    for subscription_id in subscription_ids:
        print(f"🔍 Processing subscription: {subscription_id}")
        
        try:
            workspaces = list_workspaces(subscription_id)
            total_workspaces_across_subs += len(workspaces)
            
            subscription_data = {
                "subscription_id": subscription_id,
                "total_workspaces": len(workspaces),
                "workspaces": []
            }
            
            for ws in workspaces:
                resource_group = parse_resource_group_from_id(ws.id)
                
                # Get all workspace components
                notebooks = list_notebooks(subscription_id, ws.name, resource_group, credential)
                pools = list_big_data_pools(subscription_id, resource_group, ws.name, credential)
                jobs = list_spark_job_definitions(ws.name, credential)
                
                # Count notebooks by Spark version
                nb_by_runtime = defaultdict(int)
                for nb in notebooks:
                    props = nb.get("properties", {})
                    pool = props.get("bigDataPool", {})
                    spark_version = "No pool attached"
                    if pool and "referenceName" in pool:
                        pool_name = pool["referenceName"]
                        matching_pool = next((p for p in pools if p.get("name") == pool_name), None)
                        if matching_pool:
                            spark_version = matching_pool.get("properties", {}).get("sparkVersion", "Unknown")
                    nb_by_runtime[spark_version] += 1
                
                # Count jobs by runtime
                jobs_by_runtime = defaultdict(int)
                for job in jobs:
                    runtime = job.get("properties", {}).get("requiredSparkVersion", "Unknown")
                    jobs_by_runtime[runtime] += 1
                
                # Build workspace data structure
                workspace_data = {
                    "workspace_details": extract_workspace_details(ws),
                    "resource_group": resource_group,
                    "notebooks": {
                        "count": len(notebooks),
                        "by_runtime": dict(nb_by_runtime),
                        "details": [extract_notebook_details(nb) for nb in notebooks]
                    },
                    "big_data_pools": {
                        "count": len(pools),
                        "details": [extract_big_data_pool_details(pool) for pool in pools]
                    },
                    "spark_job_definitions": {
                        "count": len(jobs),
                        "by_runtime": dict(jobs_by_runtime),
                        "details": [extract_spark_job_definition_details(job) for job in jobs]
                    },
                    "summary": {
                        "spark_pools": len(pools),
                        "notebooks": len(notebooks),
                        "notebooks_by_runtime": dict(nb_by_runtime),
                        "spark_job_definitions": len(jobs),
                        "jobs_by_runtime": dict(jobs_by_runtime)
                    }
                }
                
                subscription_data["workspaces"].append(workspace_data)
            
            consolidated_data["subscriptions"].append(subscription_data)
            
        except Exception as e:
            print(f"⚠️ Error processing subscription {subscription_id}: {e}")
            # Add error info to consolidated data
            consolidated_data["subscriptions"].append({
                "subscription_id": subscription_id,
                "error": str(e),
                "total_workspaces": 0,
                "workspaces": []
            })
    
    # Add overall summary
    consolidated_data["total_workspaces_across_all_subscriptions"] = total_workspaces_across_subs
    
    # Write to JSON file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(consolidated_data, f, indent=2, ensure_ascii=False)
    
    print(f"📄 Consolidated JSON export completed: {filename}")
    return filename

def process_subscription(subscription_id, credential):
    """Process a single subscription - print details and return data."""
    print(f"🔍 Listing all Synapse workspaces in subscription {subscription_id}...\n")
    
    try:
        workspaces = list_workspaces(subscription_id)
        
        if not workspaces:
            print("❌ No Synapse workspaces found in this subscription.")
            return False
        
        # Print all details (existing functionality)
        for ws in workspaces:
            print_workspace_details(ws)
            resource_group = parse_resource_group_from_id(ws.id)
            
            notebooks = list_notebooks(subscription_id, ws.name, resource_group, credential)
            if not notebooks:
                print("   No notebooks found.\n")
            else:
                for nb in notebooks:
                    print_notebook_details(nb)
            
            pools = list_big_data_pools(subscription_id, resource_group, ws.name, credential)
            if not pools:
                print("   No Big Data pools found.\n")
            else:
                for pool in pools:
                    print_big_data_pool_details(pool)
            
            jobs = list_spark_job_definitions(ws.name, credential)
            if not jobs:
                print("   No Spark Job Definitions found.\n")
            else:
                for job in jobs:
                    print_spark_job_definition_details(job)
        
        # Print high-level summary
        summarize_subscription(subscription_id, credential)
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing subscription {subscription_id}: {e}")
        return False

if __name__ == "__main__":
    import sys
    credential = DefaultAzureCredential()
    
    # Check if config file is provided as command line argument
    if len(sys.argv) != 2:
        print("❌ Usage: python metadata.py <config_file>")
        print("   Example: python metadata.py config.json")
        exit(1)
    
    config_file = sys.argv[1]
    
    # Load configuration
    try:
        config = load_config(config_file)
        subscription_ids = config['subscription_ids']
        
        print(f"🚀 Found {len(subscription_ids)} subscription(s) in {config_file}:")
        for sub_id in subscription_ids:
            print(f"   • {sub_id}")
        print()
        
    except FileNotFoundError as e:
        print(f"❌ {e}")
        print("\n📝 Expected config file format:")
        print('{')
        print('  "subscription_ids": [')
        print('    "your-subscription-id-1",')
        print('    "your-subscription-id-2"')
        print('  ]')
        print('}')
        exit(1)
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        exit(1)
    
    # Process each subscription
    successful_subscriptions = []
    
    for i, subscription_id in enumerate(subscription_ids, 1):
        print(f"{'='*80}")
        print(f"PROCESSING SUBSCRIPTION {i}/{len(subscription_ids)}: {subscription_id}")
        print(f"{'='*80}")
        
        success = process_subscription(subscription_id, credential)
        if success:
            successful_subscriptions.append(subscription_id)
        
        print()  # Add spacing between subscriptions
    
    # Generate JSON exports
    print(f"{'='*80}")
    print("GENERATING JSON EXPORTS")
    print(f"{'='*80}")
    
    if successful_subscriptions:
        if len(successful_subscriptions) == 1:
            # Single subscription - create individual JSON file
            subscription_id = successful_subscriptions[0]
            json_filename = write_to_json(subscription_id, credential)
            print(f"✅ Individual subscription data exported to: {json_filename}")
        
        else:
            # Multiple subscriptions - create both individual and consolidated files
            individual_files = []
            
            # Create individual files for each subscription
            for subscription_id in successful_subscriptions:
                json_filename = write_to_json(subscription_id, credential)
                individual_files.append(json_filename)
                print(f"✅ Individual subscription data exported to: {json_filename}")
            
            # Create consolidated file
            consolidated_filename = write_consolidated_json(successful_subscriptions, credential)
            print(f"✅ Consolidated data exported to: {consolidated_filename}")
            
            print(f"\n📊 EXPORT SUMMARY:")
            print(f"   • Individual files: {len(individual_files)}")
            print(f"   • Consolidated file: 1")
            print(f"   • Total files created: {len(individual_files) + 1}")
    
    else:
        print("❌ No subscriptions were processed successfully. No JSON files created.")
    
    print(f"\n🎯 FINAL SUMMARY:")
    print(f"   • Total subscriptions configured: {len(subscription_ids)}")
    print(f"   • Successfully processed: {len(successful_subscriptions)}")
    print(f"   • Failed: {len(subscription_ids) - len(successful_subscriptions)}")
    
    if len(subscription_ids) - len(successful_subscriptions) > 0:
        failed_subscriptions = [sub for sub in subscription_ids if sub not in successful_subscriptions]
        print(f"   • Failed subscription(s): {', '.join(failed_subscriptions)}")
