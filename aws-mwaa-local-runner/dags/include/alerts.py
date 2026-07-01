import logging

def send_failure_email(context):
    """
    Custom failure callback that triggers an email alert.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    
    # You can customize this logging or replace it with your actual email logic
    # For example, using smtplib or an internal API.
    message = f"Alert! Task '{task_id}' in DAG '{dag_id}' failed. Logs available at: {log_url}"
    logging.error(message)
    
    # Example snippet for sending an email:
    # from airflow.utils.email import send_email
    # send_email(
    #     to=["team@example.com"],
    #     subject=f"Airflow Task Failed: {task_id}",
    #     html_content=f"<h3>Task Failed</h3><p>DAG: {dag_id}</p><p>Logs: <a href='{log_url}'>View Logs</a></p>"
    # )
