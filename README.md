# Fabric Migration Assistant

The **Synapse Discovery Tool** is a migration assistant that helps you explore the configuration of Azure Synapse Analytics environments across one or more Azure subscriptions. 
It currently collects metadata for Synapse workspaces, notebooks, Spark pools, and Spark job definitions, and can export results in JSON for migrating to Fabric. 

---

## ✨ Features

- **Multi-subscription scanning**  
  Provide one or more Azure subscription IDs in a config file, and the tool scans all Synapse workspaces.

- **Workspace metadata**  
  Collects key properties including provisioning state, private endpoint connections, and exfilteration settings.

- **Notebook inventory**  
  Lists all notebooks within each workspace, capturing runtime details, Spark pool references, and session properties.

- **Spark pool metadata**  
  Retrieves Spark Big Data pool configurations such as:
  - Node size, count, and family
  - Spark version
  - Autoscale and dynamic executor allocation settings
  - Library requirements and custom libraries

- **Spark job definitions**  
  Captures all Spark job definitions in a workspace, including runtime, target pool, language, files, resources, and arguments.

- **Summaries & reporting**  
  - Console output with detailed workspace, notebook, pool, and job properties  
  - High-level summary of number of Notebooks, SJDs, pools in each workspace.
  - JSON export of all the details collected. 

---

## 📦 Installation

Clone the repository and install the required dependencies:

~~~bash
git clone <repo-url>
cd synapse-discovery
pip install -r requirements.txt
python fetch_all_synapse_spark_metadata.py config.json
~~~

## 🚧 Upcoming Features
- Discovery of pipelines that invoke Notebooks  
- Discovery of pipelines that invoke Spark Job Definitions (SJD) 
