"""
Unit tests for job management functionality.

Tests the job manager module and all job-related MCP tools with mocking.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import our job management modules
from src.mcp_server.job_manager import DatabricksJobManager, JobInfo, JobRunInfo
from src.mcp_server.mcp_server import (
    _list_jobs, _get_job_details, _get_job_runs, 
    _trigger_job, _cancel_job_run, _get_job_run_output
)


class TestDatabricksJobManager:
    """Test cases for the DatabricksJobManager class."""
    
    @patch.dict('os.environ', {
        'DATABRICKS_HOST': 'test-workspace.cloud.databricks.com',
        'DATABRICKS_TOKEN': 'test-token'
    })
    def test_job_manager_initialization(self):
        """Test that job manager initializes correctly with environment variables."""
        with patch('src.mcp_server.job_manager.log_databricks_event'):
            manager = DatabricksJobManager()
            
            assert manager.host == 'test-workspace.cloud.databricks.com'
            assert manager.token == 'test-token'
            assert manager.base_url == 'https://test-workspace.cloud.databricks.com/api/2.1'
            assert 'Authorization' in manager.headers
            assert manager.headers['Authorization'] == 'Bearer test-token'
    
    @patch.dict('os.environ', {}, clear=True)
    def test_job_manager_missing_credentials(self):
        """Test that job manager raises error when credentials are missing."""
        with pytest.raises(ValueError, match="DATABRICKS_HOST and DATABRICKS_TOKEN must be set"):
            DatabricksJobManager()
    
    @patch.dict('os.environ', {
        'DATABRICKS_HOST': 'https://test-workspace.cloud.databricks.com',
        'DATABRICKS_TOKEN': 'test-token'
    })
    def test_job_manager_removes_https_prefix(self):
        """Test that job manager removes https:// prefix from host."""
        with patch('src.mcp_server.job_manager.log_databricks_event'):
            manager = DatabricksJobManager()
            assert manager.host == 'test-workspace.cloud.databricks.com'
    
    @patch.dict('os.environ', {
        'DATABRICKS_HOST': 'test-workspace.cloud.databricks.com',
        'DATABRICKS_TOKEN': 'test-token'
    })
    @patch('src.mcp_server.job_manager.requests.get')
    @patch('src.mcp_server.job_manager.log_databricks_event')
    def test_list_jobs_success(self, mock_log, mock_get):
        """Test successful job listing."""
        # Mock API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobs": [
                {
                    "job_id": 123,
                    "settings": {
                        "name": "Test Job",
                        "notebook_task": {"notebook_path": "/test"}
                    },
                    "creator_user_name": "test@example.com",
                    "created_time": 1640995200000,  # 2022-01-01 00:00:00
                    "last_run": {
                        "state": {"life_cycle_state": "TERMINATED"},
                        "start_time": 1640995200000
                    }
                }
            ]
        }
        mock_get.return_value = mock_response
        
        manager = DatabricksJobManager()
        jobs = manager.list_jobs(limit=10)
        
        assert len(jobs) == 1
        assert jobs[0].job_id == 123
        assert jobs[0].name == "Test Job"
        assert jobs[0].job_type == "NOTEBOOK"
        assert jobs[0].creator_email == "test@example.com"
        assert jobs[0].last_run_state == "TERMINATED"
    
    @patch.dict('os.environ', {
        'DATABRICKS_HOST': 'test-workspace.cloud.databricks.com',
        'DATABRICKS_TOKEN': 'test-token'
    })
    @patch('src.mcp_server.job_manager.requests.get')
    @patch('src.mcp_server.job_manager.log_databricks_event')
    def test_get_job_details_success(self, mock_log, mock_get):
        """Test successful job details retrieval."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "job_id": 123,
            "settings": {
                "name": "Test Job",
                "notebook_task": {
                    "notebook_path": "/test",
                    "base_parameters": {"param1": "value1"}
                },
                "new_cluster": {
                    "spark_version": "11.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2
                },
                "schedule": {
                    "quartz_cron_expression": "0 0 12 * * ?",
                    "timezone_id": "UTC"
                },
                "timeout_seconds": 3600,
                "max_concurrent_runs": 1
            },
            "creator_user_name": "test@example.com",
            "created_time": 1640995200000
        }
        mock_get.return_value = mock_response
        
        manager = DatabricksJobManager()
        details = manager.get_job_details(123)
        
        assert details["job_id"] == 123
        assert details["name"] == "Test Job"
        assert details["job_type"] == "NOTEBOOK"
        assert details["timeout_seconds"] == 3600
        assert details["schedule"]["quartz_cron_expression"] == "0 0 12 * * ?"
        assert details["cluster_config"]["type"] == "new"
        assert details["task_config"]["notebook_path"] == "/test"
    
    @patch.dict('os.environ', {
        'DATABRICKS_HOST': 'test-workspace.cloud.databricks.com',
        'DATABRICKS_TOKEN': 'test-token'
    })
    @patch('src.mcp_server.job_manager.requests.post')
    @patch('src.mcp_server.job_manager.log_databricks_event')
    def test_trigger_job_success(self, mock_log, mock_post):
        """Test successful job triggering."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.status_code = 200
        mock_response.json.return_value = {"run_id": 456789}
        mock_post.return_value = mock_response
        
        manager = DatabricksJobManager()
        run_id = manager.trigger_job(123, notebook_params={"param1": "value1"})
        
        assert run_id == 456789
        # Verify the API was called with correct data
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]['json']['job_id'] == 123
        assert call_args[1]['json']['notebook_params'] == {"param1": "value1"}
    
    @patch.dict('os.environ', {
        'DATABRICKS_HOST': 'test-workspace.cloud.databricks.com',
        'DATABRICKS_TOKEN': 'test-token'
    })
    @patch('src.mcp_server.job_manager.requests.post')
    @patch('src.mcp_server.job_manager.log_databricks_event')
    def test_cancel_job_run_success(self, mock_log, mock_post):
        """Test successful job run cancellation."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_post.return_value = mock_response
        
        manager = DatabricksJobManager()
        result = manager.cancel_job_run(456789)
        
        assert result is True
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]['json']['run_id'] == 456789
    
    def test_determine_job_type(self):
        """Test job type determination from settings."""
        with patch('src.mcp_server.job_manager.log_databricks_event'):
            manager = DatabricksJobManager.__new__(DatabricksJobManager)
            
            assert manager._determine_job_type({"notebook_task": {}}) == "NOTEBOOK"
            assert manager._determine_job_type({"spark_jar_task": {}}) == "JAR"
            assert manager._determine_job_type({"spark_python_task": {}}) == "PYTHON"
            assert manager._determine_job_type({"sql_task": {}}) == "SQL"
            assert manager._determine_job_type({"pipeline_task": {}}) == "PIPELINE"
            assert manager._determine_job_type({}) == "UNKNOWN"
    
    def test_format_timestamp(self):
        """Test timestamp formatting."""
        with patch('src.mcp_server.job_manager.log_databricks_event'):
            manager = DatabricksJobManager.__new__(DatabricksJobManager)
            
            # Test with valid timestamp (2022-01-01 00:00:00 UTC)
            formatted = manager._format_timestamp(1640995200000)
            assert "2022-01-01" in formatted
            
            # Test with None/0
            assert manager._format_timestamp(0) == "Unknown"
            assert manager._format_timestamp(None) == "Unknown"


class TestMCPJobTools:
    """Test cases for the MCP job management tools."""
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_list_jobs_mcp_tool(self, mock_log, mock_get_manager):
        """Test the list_jobs MCP tool."""
        # Mock job manager
        mock_manager = Mock()
        mock_jobs = [
            JobInfo(
                job_id=123,
                name="Test Job",
                creator_email="test@example.com",
                created_time=1640995200000,
                job_type="NOTEBOOK",
                status="ACTIVE",
                last_run_state="TERMINATED"
            )
        ]
        mock_manager.list_jobs.return_value = mock_jobs
        mock_get_manager.return_value = mock_manager
        
        result = _list_jobs(limit=10, name_filter="test")
        
        assert "Databricks Jobs (1 found)" in result
        assert "123" in result
        assert "Test Job" in result
        assert "NOTEBOOK" in result
        assert "TERMINATED" in result
        mock_manager.list_jobs.assert_called_once_with(limit=10, name_filter="test")
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_get_job_details_mcp_tool(self, mock_log, mock_get_manager):
        """Test the get_job_details MCP tool."""
        mock_manager = Mock()
        mock_details = {
            "job_id": 123,
            "name": "Test Job",
            "job_type": "NOTEBOOK",
            "creator": "test@example.com",
            "created_time": "2022-01-01 00:00:00 UTC",
            "timeout_seconds": 3600,
            "max_concurrent_runs": 1,
            "schedule": {
                "quartz_cron_expression": "0 0 12 * * ?",
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED"
            },
            "cluster_config": {"type": "new", "spark_version": "11.3.x"},
            "task_config": {"type": "NOTEBOOK", "notebook_path": "/test"}
        }
        mock_manager.get_job_details.return_value = mock_details
        mock_get_manager.return_value = mock_manager
        
        result = _get_job_details(123)
        
        assert "Job Details for ID 123" in result
        assert "Test Job" in result
        assert "NOTEBOOK" in result
        assert "Schedule" in result
        assert "Cluster Configuration" in result
        mock_manager.get_job_details.assert_called_once_with(123)
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_trigger_job_mcp_tool(self, mock_log, mock_get_manager):
        """Test the trigger_job MCP tool."""
        mock_manager = Mock()
        mock_manager.trigger_job.return_value = 456789
        mock_get_manager.return_value = mock_manager
        
        # Test with valid JSON parameters
        result = _trigger_job(123, notebook_params='{"param1": "value1"}')
        
        assert "Job 123 triggered successfully" in result
        assert "Run ID**: 456789" in result
        mock_manager.trigger_job.assert_called_once_with(
            job_id=123,
            notebook_params={"param1": "value1"},
            jar_params=None,
            python_params=None
        )
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_trigger_job_invalid_json(self, mock_log, mock_get_manager):
        """Test trigger_job with invalid JSON parameters."""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        
        result = _trigger_job(123, notebook_params='invalid json')
        
        assert "Error: notebook_params must be valid JSON string" in result
        # Manager should not be called with invalid JSON
        mock_manager.trigger_job.assert_not_called()
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_cancel_job_run_mcp_tool(self, mock_log, mock_get_manager):
        """Test the cancel_job_run MCP tool."""
        mock_manager = Mock()
        mock_manager.cancel_job_run.return_value = True
        mock_get_manager.return_value = mock_manager
        
        result = _cancel_job_run(456789)
        
        assert "Job run 456789 cancelled successfully" in result
        assert "Cancellation request sent" in result
        mock_manager.cancel_job_run.assert_called_once_with(456789)
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_get_job_run_output_mcp_tool(self, mock_log, mock_get_manager):
        """Test the get_job_run_output MCP tool."""
        mock_manager = Mock()
        mock_output = {
            "run_id": 456789,
            "logs": "Job executed successfully",
            "error": "",
            "metadata": {"duration": 120},
            "notebook_output": {"result": "success"},
            "error_trace": "",
            "logs_truncated": False
        }
        mock_manager.get_job_run_output.return_value = mock_output
        mock_get_manager.return_value = mock_manager
        
        result = _get_job_run_output(456789)
        
        assert "Job Run Output for Run ID 456789" in result
        assert "Job executed successfully" in result
        assert "Notebook Output" in result
        assert "Metadata" in result
        mock_manager.get_job_run_output.assert_called_once_with(456789)
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_list_jobs_no_results(self, mock_log, mock_get_manager):
        """Test list_jobs when no jobs are found."""
        mock_manager = Mock()
        mock_manager.list_jobs.return_value = []
        mock_get_manager.return_value = mock_manager
        
        result = _list_jobs()
        
        assert "No jobs found in the Databricks workspace" in result
    
    @patch('src.mcp_server.mcp_server.get_job_manager')
    @patch('src.mcp_server.mcp_server.log_mcp_event')
    def test_job_tool_error_handling(self, mock_log, mock_get_manager):
        """Test error handling in job tools."""
        mock_manager = Mock()
        mock_manager.list_jobs.side_effect = Exception("API Error")
        mock_get_manager.return_value = mock_manager
        
        result = _list_jobs()
        
        assert "Error listing jobs: API Error" in result
        # Verify error was logged
        mock_log.assert_called()
        error_calls = [call for call in mock_log.call_args_list if 'ERROR' in call[0]]
        assert len(error_calls) > 0


class TestJobDataClasses:
    """Test cases for job data classes."""
    
    def test_job_info_creation(self):
        """Test JobInfo data class creation."""
        job = JobInfo(
            job_id=123,
            name="Test Job",
            creator_email="test@example.com",
            created_time=1640995200000,
            job_type="NOTEBOOK",
            status="ACTIVE",
            last_run_state="TERMINATED",
            last_run_time=1640995200000
        )
        
        assert job.job_id == 123
        assert job.name == "Test Job"
        assert job.job_type == "NOTEBOOK"
        assert job.last_run_state == "TERMINATED"
    
    def test_job_run_info_creation(self):
        """Test JobRunInfo data class creation."""
        run = JobRunInfo(
            run_id=456789,
            job_id=123,
            run_name="Test Run",
            state="TERMINATED",
            life_cycle_state="TERMINATED",
            result_state="SUCCESS",
            start_time=1640995200000,
            end_time=1640995260000,
            execution_duration=60000,
            trigger="MANUAL"
        )
        
        assert run.run_id == 456789
        assert run.job_id == 123
        assert run.state == "TERMINATED"
        assert run.result_state == "SUCCESS"
        assert run.execution_duration == 60000


if __name__ == "__main__":
    pytest.main([__file__]) 