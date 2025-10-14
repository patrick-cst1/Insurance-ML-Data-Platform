"""
Fabric Deployment Script
Deploy notebooks and configurations to Microsoft Fabric workspace using REST API
"""

import requests
import yaml
import os
import sys
import json
import base64
from typing import Dict, List
from pathlib import Path


class FabricDeployer:
    """Deploy artifacts to Microsoft Fabric workspace."""
    
    def __init__(self, workspace_id: str, token: str):
        self.workspace_id = workspace_id
        self.token = token
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def upload_notebook(self, notebook_path: str, display_name: str) -> Dict:
        """
        Upload notebook to Fabric workspace.
        
        Args:
            notebook_path: Local path to notebook file
            display_name: Display name in Fabric
        
        Returns:
            API response dict
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        
        # Read notebook content
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook_content = f.read()
        
        # Encode content
        encoded_content = base64.b64encode(notebook_content.encode()).decode()
        
        payload = {
            "displayName": display_name,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": encoded_content,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        
        if response.status_code in [200, 201]:
            print(f"✓ Uploaded notebook: {display_name}")
            return response.json()
        else:
            print(f"✗ Failed to upload {display_name}: {response.text}")
            return {}
    
    def create_pipeline(self, pipeline_def: Dict, display_name: str) -> Dict:
        """
        Create Data Factory pipeline in Fabric.
        
        Args:
            pipeline_def: Pipeline definition JSON
            display_name: Pipeline display name
        
        Returns:
            API response dict
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/dataPipelines"
        
        payload = {
            "displayName": display_name,
            "definition": pipeline_def
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        
        if response.status_code in [200, 201]:
            print(f"✓ Created pipeline: {display_name}")
            return response.json()
        else:
            print(f"✗ Failed to create pipeline {display_name}: {response.text}")
            return {}
    
    def list_workspace_items(self) -> List[Dict]:
        """List all items in workspace."""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return response.json().get("value", [])
        else:
            print(f"✗ Failed to list workspace items: {response.text}")
            return []


def deploy_notebooks(deployer: FabricDeployer, notebook_dir: str):
    """Deploy all notebooks from directory."""
    print("\n" + "=" * 60)
    print("Deploying Notebooks")
    print("=" * 60)
    
    notebook_paths = list(Path(notebook_dir).rglob("*.py"))
    
    for notebook_path in notebook_paths:
        relative_path = notebook_path.relative_to(notebook_dir)
        display_name = str(relative_path).replace("\\", "_").replace("/", "_").replace(".py", "")
        
        deployer.upload_notebook(str(notebook_path), display_name)


def deploy_pipelines(deployer: FabricDeployer, pipeline_dir: str):
    """Deploy all pipelines from directory."""
    print("\n" + "=" * 60)
    print("Deploying Pipelines")
    print("=" * 60)
    
    pipeline_files = list(Path(pipeline_dir).rglob("*.json"))
    
    for pipeline_file in pipeline_files:
        with open(pipeline_file, 'r') as f:
            pipeline_def = json.load(f)
        
        display_name = pipeline_def.get("name", pipeline_file.stem)
        deployer.create_pipeline(pipeline_def, display_name)


def main():
    """Main deployment routine."""
    # Load configuration
    config_path = "devops/parameters/fabric.yml"
    
    if not os.path.exists(config_path):
        print(f"✗ Configuration file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Get credentials from environment
    workspace_id = config["workspace"]["id"]
    fabric_token = os.environ.get("FABRIC_TOKEN")
    
    if not fabric_token:
        print("✗ FABRIC_TOKEN environment variable not set")
        sys.exit(1)
    
    print("=" * 60)
    print("Fabric Deployment Started")
    print("=" * 60)
    print(f"Workspace: {config['workspace']['name']}")
    print(f"Workspace ID: {workspace_id}")
    print("=" * 60)
    
    # Initialize deployer
    deployer = FabricDeployer(workspace_id, fabric_token)
    
    # List existing items
    print("\nExisting workspace items:")
    items = deployer.list_workspace_items()
    for item in items:
        print(f"  - {item.get('displayName')} ({item.get('type')})")
    
    # Deploy notebooks
    deploy_notebooks(deployer, "lakehouse")
    
    # Deploy pipelines
    deploy_pipelines(deployer, "pipelines")
    
    print("\n" + "=" * 60)
    print("✓ Deployment Completed Successfully")
    print("=" * 60)


if __name__ == "__main__":
    main()
