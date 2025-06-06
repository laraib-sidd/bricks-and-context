"""
Databricks Job Management Module for MCP Server.

This module provides comprehensive job management capabilities for AI assistants
to interact with Databricks jobs through the REST API.
"""

import json
import os
import requests
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from .logger import log_databricks_event, logger


@dataclass
class JobInfo:
    """Data class for job information."""
    job_id: int
    name: str
    creator_email: str
    created_time: int
    job_type: str
    status: str
    last_run_state: Optional[str] = None
    last_run_time: Optional[int] = None
    

@dataclass  
class JobRunInfo:
    """Data class for job run information."""
    run_id: int
    job_id: int
    run_name: str
    state: str
    life_cycle_state: str
    result_state: Optional[str]
    start_time: int
    end_time: Optional[int]
    execution_duration: Optional[int]
    trigger: str


class DatabricksJobManager:
    """
    Manages Databricks job operations through the REST API.
    
    Provides AI assistants with capabilities to:
    - List and filter jobs
    - Get detailed job information
    - Monitor job runs and status
    - Trigger job executions
    - Cancel running jobs
    - Retrieve job outputs and logs
    """
    
    def __init__(self):
        """Initialize the job manager with Databricks connection details."""
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        
        if not self.host or not self.token:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
            
        # Remove https:// if present
        if self.host.startswith("https://"):
            self.host = self.host[8:]
        elif self.host.startswith("http://"):
            self.host = self.host[7:]
            
        self.base_url = f"https://{self.host}/api/2.1"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        log_databricks_event("JOBS", "INIT", f"Job manager initialized for {self.host}")
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a request to the Databricks API.
        
        Args:
            method: HTTP method (GET, POST, DELETE)
            endpoint: API endpoint path
            data: Request data for POST requests
            
        Returns:
            API response as dictionary
            
        Raises:
            Exception: If API request fails
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            log_databricks_event("JOBS", "REQUEST", f"{method} {endpoint}")
            
            if method == "GET":
                response = requests.get(url, headers=self.headers, timeout=30)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=data, timeout=30)
            elif method == "DELETE":
                response = requests.delete(url, headers=self.headers, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
            response.raise_for_status()
            result = response.json() if response.content else {}
            
            log_databricks_event("JOBS", "SUCCESS", f"{method} {endpoint} - Status: {response.status_code}")
            return result
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {e}"
            log_databricks_event("JOBS", "ERROR", error_msg, "ERROR")
            raise Exception(error_msg)
    
    def list_jobs(self, limit: int = 25, name_filter: Optional[str] = None) -> List[JobInfo]:
        """
        List Databricks jobs with optional filtering.
        
        Args:
            limit: Maximum number of jobs to return (default: 25, max: 100)
            name_filter: Optional filter for job names (case-insensitive partial match)
            
        Returns:
            List of JobInfo objects
        """
        try:
            # Limit the number of results to prevent overwhelming responses
            limit = min(limit, 100)
            
            params = {"limit": limit}
            if name_filter:
                params["name"] = name_filter
                
            endpoint = "/jobs/list"
            if params:
                param_str = "&".join([f"{k}={v}" for k, v in params.items()])
                endpoint += f"?{param_str}"
                
            response = self._make_request("GET", endpoint)
            
            jobs = []
            for job_data in response.get("jobs", []):
                settings = job_data.get("settings", {})
                created_time = job_data.get("created_time", 0)
                
                # Get last run information if available
                last_run_state = None
                last_run_time = None
                if "last_run" in job_data:
                    last_run = job_data["last_run"]
                    last_run_state = last_run.get("state", {}).get("life_cycle_state")
                    last_run_time = last_run.get("start_time")
                
                job_info = JobInfo(
                    job_id=job_data["job_id"],
                    name=settings.get("name", f"Job {job_data['job_id']}"),
                    creator_email=job_data.get("creator_user_name", "unknown"),
                    created_time=created_time,
                    job_type=self._determine_job_type(settings),
                    status="ACTIVE" if job_data.get("settings") else "UNKNOWN",
                    last_run_state=last_run_state,
                    last_run_time=last_run_time
                )
                jobs.append(job_info)
            
            log_databricks_event("JOBS", "LIST", f"Retrieved {len(jobs)} jobs")
            return jobs
            
        except Exception as e:
            log_databricks_event("JOBS", "ERROR", f"Failed to list jobs: {e}", "ERROR")
            raise
    
    def get_job_details(self, job_id: int) -> Dict[str, Any]:
        """
        Get detailed information about a specific job.
        
        Args:
            job_id: Databricks job ID
            
        Returns:
            Detailed job information
        """
        try:
            response = self._make_request("GET", f"/jobs/get?job_id={job_id}")
            
            job_data = response
            settings = job_data.get("settings", {})
            
            # Format the response for AI consumption
            details = {
                "job_id": job_data["job_id"],
                "name": settings.get("name", f"Job {job_id}"),
                "created_time": self._format_timestamp(job_data.get("created_time", 0)),
                "creator": job_data.get("creator_user_name", "unknown"),
                "job_type": self._determine_job_type(settings),
                "schedule": self._extract_schedule_info(settings),
                "cluster_config": self._extract_cluster_info(settings),
                "task_config": self._extract_task_info(settings),
                "timeout_seconds": settings.get("timeout_seconds"),
                "max_concurrent_runs": settings.get("max_concurrent_runs", 1),
                "email_notifications": settings.get("email_notifications", {}),
                "webhook_notifications": settings.get("webhook_notifications", {}),
                "access_control_list": job_data.get("access_control_list", [])
            }
            
            log_databricks_event("JOBS", "DETAILS", f"Retrieved details for job {job_id}")
            return details
            
        except Exception as e:
            log_databricks_event("JOBS", "ERROR", f"Failed to get job {job_id} details: {e}", "ERROR")
            raise
    
    def get_job_runs(self, job_id: int, limit: int = 10, active_only: bool = False) -> List[JobRunInfo]:
        """
        Get run history for a specific job.
        
        Args:
            job_id: Databricks job ID
            limit: Maximum number of runs to return (default: 10, max: 100)
            active_only: If True, only return active (running) jobs
            
        Returns:
            List of JobRunInfo objects
        """
        try:
            limit = min(limit, 100)
            
            params = {"job_id": job_id, "limit": limit}
            if active_only:
                params["active_only"] = "true"
                
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            endpoint = f"/jobs/runs/list?{param_str}"
            
            response = self._make_request("GET", endpoint)
            
            runs = []
            for run_data in response.get("runs", []):
                state = run_data.get("state", {})
                
                run_info = JobRunInfo(
                    run_id=run_data["run_id"],
                    job_id=run_data["job_id"],
                    run_name=run_data.get("run_name", f"Run {run_data['run_id']}"),
                    state=state.get("life_cycle_state", "UNKNOWN"),
                    life_cycle_state=state.get("life_cycle_state", "UNKNOWN"),
                    result_state=state.get("result_state"),
                    start_time=run_data.get("start_time", 0),
                    end_time=run_data.get("end_time"),
                    execution_duration=run_data.get("execution_duration"),
                    trigger=run_data.get("trigger", "UNKNOWN")
                )
                runs.append(run_info)
            
            log_databricks_event("JOBS", "RUNS", f"Retrieved {len(runs)} runs for job {job_id}")
            return runs
            
        except Exception as e:
            log_databricks_event("JOBS", "ERROR", f"Failed to get runs for job {job_id}: {e}", "ERROR")
            raise
    
    def trigger_job(self, job_id: int, notebook_params: Optional[Dict] = None, 
                   jar_params: Optional[List] = None, python_params: Optional[List] = None) -> int:
        """
        Trigger a job run.
        
        Args:
            job_id: Databricks job ID
            notebook_params: Parameters for notebook tasks
            jar_params: Parameters for JAR tasks
            python_params: Parameters for Python tasks
            
        Returns:
            Run ID of the triggered job
        """
        try:
            data = {"job_id": job_id}
            
            if notebook_params:
                data["notebook_params"] = notebook_params
            if jar_params:
                data["jar_params"] = jar_params
            if python_params:
                data["python_params"] = python_params
            
            response = self._make_request("POST", "/jobs/run-now", data)
            run_id = response["run_id"]
            
            log_databricks_event("JOBS", "TRIGGER", f"Started job {job_id}, run ID: {run_id}")
            return run_id
            
        except Exception as e:
            log_databricks_event("JOBS", "ERROR", f"Failed to trigger job {job_id}: {e}", "ERROR")
            raise
    
    def cancel_job_run(self, run_id: int) -> bool:
        """
        Cancel a running job.
        
        Args:
            run_id: Job run ID to cancel
            
        Returns:
            True if cancellation was successful
        """
        try:
            data = {"run_id": run_id}
            self._make_request("POST", "/jobs/runs/cancel", data)
            
            log_databricks_event("JOBS", "CANCEL", f"Cancelled job run {run_id}")
            return True
            
        except Exception as e:
            log_databricks_event("JOBS", "ERROR", f"Failed to cancel job run {run_id}: {e}", "ERROR")
            raise
    
    def get_job_run_output(self, run_id: int) -> Dict[str, Any]:
        """
        Get output and logs from a job run.
        
        Args:
            run_id: Job run ID
            
        Returns:
            Job run output and metadata
        """
        try:
            response = self._make_request("GET", f"/jobs/runs/get-output?run_id={run_id}")
            
            # Format for AI consumption
            output = {
                "run_id": run_id,
                "logs": response.get("logs", ""),
                "error": response.get("error", ""),
                "metadata": response.get("metadata", {}),
                "notebook_output": response.get("notebook_output", {}),
                "error_trace": response.get("error_trace", ""),
                "logs_truncated": response.get("logs_truncated", False)
            }
            
            log_databricks_event("JOBS", "OUTPUT", f"Retrieved output for job run {run_id}")
            return output
            
        except Exception as e:
            log_databricks_event("JOBS", "ERROR", f"Failed to get output for job run {run_id}: {e}", "ERROR")
            raise
    
    def _determine_job_type(self, settings: Dict) -> str:
        """Determine the type of job based on its settings."""
        if "notebook_task" in settings:
            return "NOTEBOOK"
        elif "spark_jar_task" in settings:
            return "JAR"
        elif "spark_python_task" in settings:
            return "PYTHON"
        elif "spark_submit_task" in settings:
            return "SPARK_SUBMIT"
        elif "pipeline_task" in settings:
            return "PIPELINE"
        elif "python_wheel_task" in settings:
            return "PYTHON_WHEEL"
        elif "sql_task" in settings:
            return "SQL"
        else:
            return "UNKNOWN"
    
    def _extract_schedule_info(self, settings: Dict) -> Optional[Dict]:
        """Extract schedule information from job settings."""
        schedule = settings.get("schedule")
        if schedule:
            return {
                "quartz_cron_expression": schedule.get("quartz_cron_expression"),
                "timezone_id": schedule.get("timezone_id"),
                "pause_status": schedule.get("pause_status", "UNPAUSED")
            }
        return None
    
    def _extract_cluster_info(self, settings: Dict) -> Dict:
        """Extract cluster configuration information."""
        if "existing_cluster_id" in settings:
            return {"type": "existing", "cluster_id": settings["existing_cluster_id"]}
        elif "new_cluster" in settings:
            cluster = settings["new_cluster"]
            return {
                "type": "new",
                "spark_version": cluster.get("spark_version"),
                "node_type_id": cluster.get("node_type_id"),
                "num_workers": cluster.get("num_workers"),
                "autoscale": cluster.get("autoscale")
            }
        elif "job_cluster_key" in settings:
            return {"type": "job_cluster", "key": settings["job_cluster_key"]}
        else:
            return {"type": "unknown"}
    
    def _extract_task_info(self, settings: Dict) -> Dict:
        """Extract task configuration information."""
        task_info = {"type": self._determine_job_type(settings)}
        
        # Add specific task details based on type
        if "notebook_task" in settings:
            task = settings["notebook_task"]
            task_info.update({
                "notebook_path": task.get("notebook_path"),
                "base_parameters": task.get("base_parameters", {})
            })
        elif "spark_python_task" in settings:
            task = settings["spark_python_task"]
            task_info.update({
                "python_file": task.get("python_file"),
                "parameters": task.get("parameters", [])
            })
        elif "sql_task" in settings:
            task = settings["sql_task"]
            task_info.update({
                "warehouse_id": task.get("warehouse_id"),
                "query": task.get("query", {})
            })
        
        return task_info
    
    def _format_timestamp(self, timestamp_ms: int) -> str:
        """Format timestamp for human readability."""
        if timestamp_ms:
            dt = datetime.fromtimestamp(timestamp_ms / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        return "Unknown"


# Global job manager instance
job_manager = None

def get_job_manager() -> DatabricksJobManager:
    """Get or create the global job manager instance."""
    global job_manager
    if job_manager is None:
        job_manager = DatabricksJobManager()
    return job_manager 