# Local Development Guide (MWAA & Airflow)

Welcome to the project! This guide will walk you through setting up your local development environment so you can run, test, and develop Airflow DAGs locally using the MWAA (Managed Workflows for Apache Airflow) local runner.

## Prerequisites

Before getting started, make sure you have the following installed on your machine:
1. **Docker Desktop** (Make sure it is running)
2. **Git**
3. **PowerShell** (for Windows) or **Bash** (for Mac/Linux)

---

## Step 1: Clone the Repository

If you haven't already, clone the project repository to your local machine and navigate into the root directory:

```bash
git clone <repository_url>
cd docker_mwaa_google
```

---

## Step 2: Build the MWAA Docker Image (Skip if pre-pulled)

The AWS MWAA local runner requires a custom Docker image that mimics the production MWAA environment. You only need to do this once.

> [!NOTE]
> **Office Network Restrictions:** If your company network restricts building Docker images and a senior developer has already manually pulled the pre-built image (e.g. `vinayram21/mwaa-custom-env:latest`) to your machine, you can safely **skip this entire step** and proceed directly to Step 3.

1. Navigate to the `aws-mwaa-local-runner` folder:
   ```bash
   cd aws-mwaa-local-runner
   ```
2. Build the local environment image. This process might take 20-40 minutes depending on your internet speed and machine:
   ```bash
   ./mwaa-local-env build-image
   ```
   *(Note: If you are on Windows, you can run this command in WSL, Git Bash, or just rely on the pre-built images if configured).*

---

## Step 3: Spin Up the Local Docker Environment

Once the image is built, you can start the Airflow database (Postgres) and the Airflow Webserver/Scheduler (Local Runner).

Make sure you are still inside the `aws-mwaa-local-runner` directory, then run:

```bash
docker compose -f docker/docker-compose-local.yml up -d
```

- `-f docker/docker-compose-local.yml` points to our local configuration.
- `up -d` starts the containers in detached mode (in the background).

---

## Step 4: Access the Airflow UI

Wait about 1-2 minutes for the scheduler and webserver to fully boot up. 

Open your web browser and navigate to:
**http://localhost:8080**

- **Username:** `admin`
- **Password:** `test`

You should now see the Airflow Dashboard with all the DAGs loaded!

---

## Step 5: How to Interact with Airflow Locally

### Viewing Logs
If something goes wrong or the webserver doesn't start, you can check the container logs:
```bash
docker logs docker-local-runner-1 -f
```
*(Press `Ctrl+C` to exit the logs).*

### Testing a DAG or Task
You can run Airflow CLI commands directly inside the running container. This is useful for testing specific tasks or checking if your DAGs parse correctly without syntax errors.

**List all DAGs:**
```bash
docker exec docker-local-runner-1 airflow dags list
```

**Check for DAG import errors:**
```bash
docker exec docker-local-runner-1 airflow dags list-import-errors
```

**Test a specific task:**
*(Format: airflow tasks test <dag_id> <task_id> <execution_date>)*
```bash
docker exec docker-local-runner-1 airflow tasks test dag_a hello_add 2025-01-01
```

---

## Step 6: Shutting Down

When you are done for the day and want to stop the local environment and free up resources on your machine, run:

```bash
docker compose -f docker/docker-compose-local.yml down
```

*(This stops and removes the running containers but preserves your database data in the mounted volume).*

## Troubleshooting

- **No such table: dag / SQLAlchemy errors:** This usually happens if you run CLI commands improperly without environment variables. Use `docker exec` against the running `docker-local-runner-1` container as shown above.
- **Port 8080 already in use:** If Airflow fails to bind to port 8080, make sure no other application (like Tomcat or another web server) is running on port 8080.
- **DAGs are not showing up:** Wait a couple of minutes. The Airflow scheduler parses the `dags/` folder every few minutes. Ensure your YAML or Python files are properly saved in `aws-mwaa-local-runner/dags`.
