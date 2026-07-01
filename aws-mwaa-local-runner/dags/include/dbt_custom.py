from typing import Any, Optional, List
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook

class DbtCloudRunJobByNameOperator(DbtCloudRunJobOperator):
    """
    Subclass of DbtCloudRunJobOperator that dynamically looks up a job_id by job_name before executing.
    This avoids needing to hardcode job IDs.
    """
    def __init__(self, *, job_name: str, **kwargs):
        # We pass a dummy job_id to satisfy the parent class requirement during initialization
        kwargs['job_id'] = kwargs.get('job_id', 0)
        super().__init__(**kwargs)
        self.job_name = job_name

    def execute(self, context: dict):
        hook = DbtCloudHook(self.dbt_cloud_conn_id)
        
        # Determine the account ID
        account_id = self.account_id
        if not account_id:
            conn = hook.get_connection(self.dbt_cloud_conn_id)
            account_id = conn.login

        if not account_id:
            raise ValueError("account_id must be provided or set as login in the connection")

        self.log.info(f"Looking up dbt Cloud Job ID for job name: '{self.job_name}'")
        
        responses = hook.list_jobs(account_id=int(account_id))
        
        found_job_id = None
        for resp in responses:
            if hasattr(resp, 'json'):
                data = resp.json().get('data', [])
            else:
                data = resp.get('data', []) if isinstance(resp, dict) else resp

            for job in data:
                if job.get('name') == self.job_name:
                    found_job_id = job.get('id')
                    break
            
            if found_job_id:
                break

        if not found_job_id:
            raise ValueError(f"Could not find a dbt Cloud Job with name '{self.job_name}'")

        self.log.info(f"Found job_id {found_job_id} for job_name '{self.job_name}'")
        
        # Override the dummy job_id with the real one before parent execute runs
        self.job_id = found_job_id
        
        return super().execute(context)
