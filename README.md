# Equipment Failure Prediction Application

**A scalable machine learning pipeline for on-demand and scheduled predictions, featuring real-time data ingestion, quality validation, and monitoring—all powered by Docker and AWS cloud technologies..**

---
## Table of Contents
1. [Introduction](#introduction)
2. [Features](#features)
3. [Architecture](#architecture)
4. [Technologies Used](#technologies-used)
5. [Installation](#installation)
6. [Usage](#usage)
7. [Project Structure](#project-structure)
8. [cknowledgments](#acknowledgments)
---

## Introduction
A comprehensive machine learning pipeline designed to:
- Provide **on-demand predictions** via a user-friendly web interface.
- Automate **scheduled predictions** using Airflow.
- Ingest and validate data quality using **Great Expectations** or **TensorFlow Data Validation**.
- Monitor data quality and model performance in real-time using **Grafana** dashboards.
- Store predictions and data quality issues in a **PostgreSQL** database.

The pipeline is built with modularity and scalability in mind, leveraging **Docker** for containerization and **AWS** for cloud deployment. It serves as a robust solution for deploying machine learning models in production while ensuring data quality and performance monitoring.

---

## Features
- **User Interface**:
  - Make single or batch predictions via a Streamlit web app.
  - View past predictions with filtering by date and prediction source (web app or scheduled job).
- **Model API**:
  - Expose the ML model via FastAPI for predictions.
  - Save predictions and used features to the database.
  - Retrieve past predictions for visualization.
- **Database**:
  - Store predictions, data quality issues, and ingestion statistics.
- **Data Ingestion Job**:
  - Simulate continuous data flow by ingesting files every minute.
  - Validate data quality and raise alerts for issues.
  - Split data into `good_data` and `bad_data` based on quality.
- **Prediction Job**:
  - Automate predictions every 2 minutes using Airflow.
  - Skip execution if no new data is available.
- **Monitoring Dashboards**:
  - Monitor data quality issues and model performance in real-time using Grafana.
  - Set up alerts for critical issues (e.g., all ingested data has errors, model predicting zero).

---
## Architecture
The project architecture consists of the following components:
1. **User Interface**: Streamlit web app for on-demand predictions and visualization.
2. **Model API**: FastAPI service for serving predictions and saving results.
3. **Database**: PostgreSQL database for storing predictions, data quality issues, and statistics.
4. **Data Ingestion Job**: Airflow DAG for ingesting and validating data.
5. **Prediction Job**: Airflow DAG for scheduled predictions.
6. **Monitoring Dashboards**: Grafana dashboards for real-time monitoring.

![Project Architecture](./overview.drawio.png)*An overview of data flow*

### How Components Interact:
- The **Streamlit UI** interacts with the **FastAPI** service to make predictions and retrieve past predictions.
- The **FastAPI** service saves predictions and used features to the **PostgreSQL** database.
- The **Data Ingestion Job** reads data from the `raw-data` folder, validates it, and splits it into `good_data` and `bad_data` folders.
- The **Prediction Job** checks the `good_data` folder for new files and makes predictions via the **FastAPI** service.
- The **Grafana** dashboards query the database to display real-time insights into data quality and model performance.
---
## Technologies Used
- **Programming Languages**:
  - Python (primary language for all components)
- **Frameworks/Libraries**:
  - Streamlit (User Interface)
  - FastAPI (Model API)
  - PostgreSQL (Database)
  - SQLAlchemy (ORM for database interactions)
  - Airflow (Scheduling for data ingestion and prediction jobs)
  - Great Expectations / TensorFlow Data Validation (Data Validation)
  - Grafana (Monitoring Dashboards)
- **Containerization**:
  - Docker (Containerization of all services)
  - Docker Compose (Orchestration of multi-container setup)
- **Cloud Technologies**:
  - AWS (EC2, S3, RDS, etc. for deployment and storage)
- **Other Tools**:
  - Git (Version control)
  - GitHub (Repository hosting)
  - Pandas, NumPy, Scikit-learn (Data processing and ML modeling)
___

## Installation
To set up the project locally, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/your-repo-name.git

2. **Navigate to the project directory**:
    ```
    cd your-repo-name
3. **Set up environment variables**:
    - Create a .env file in the root directory.
    - Add the required environment variables (e.g., database credentials, API keys, etc.). Example:
    ```
    POSTGRES_USER=your_db_user
    POSTGRES_PASSWORD=your_db_password
    POSTGRES_DB=your_db_name
4. **Build and start Docker containers**:
    ```
    docker-compose up --build
5. **Access the services**:
    - Streamlit UI: Open your browser and go to http://localhost:8501.
    - FastAPI Docs: Open your browser and go to http://localhost:8000/docs
    - Grafana Dashboard: Open your browser and go to http://localhost:3000.
    - Airflow UI: Open your browser and go to http://localhost:8080.
6. **Verify the setup**:
    - Check the logs in your terminal to ensure all services are running without errors.
    - Test the Streamlit UI and FastAPI endpoints to confirm everything is working as expected.
---
Note:
- Ensure Docker and Docker Compose are installed on your system.
- If you encounter issues, check the logs for specific error messages and troubleshoot accordingly.
---
## Usage
### User Interface (Streamlit)
1. **Access the Streamlit UI**:
   - Open your browser and navigate to `http://localhost:8501`.

2. **Make Predictions**:
   - **Single Prediction**:
     - Fill in the feature values in the form.
     - Click the "Predict" button to get the prediction.
   - **Batch Prediction**:
     - Upload a CSV file containing the feature values.
     - Click the "Predict" button to get predictions for all rows in the file.

3. **View Past Predictions**:
   - Select a date range and prediction source (web app, scheduled job, or all).
   - View the past predictions in a table format.

---

### Model API (FastAPI)
1. **Access the API Documentation**:
   - Open your browser and navigate to `http://localhost:8000/docs`.

2. **Endpoints**:
   - **`/predict`**:
     - Make predictions by sending a POST request with feature data (single or batch).
     - Example request:
       ```json
       {
         "features": [
           {"feature1": value1, "feature2": value2, ...},
           {"feature1": value3, "feature2": value4, ...}
         ]
       }
       ```
   - **`/past-predictions`**:
     - Retrieve past predictions by sending a GET request with optional filters (date range, source).
     - Example request:
       ```json
       {
         "start_date": "2023-10-01",
         "end_date": "2023-10-31",
         "source": "webapp"
       }
       ```

---

### Data Ingestion Job (Airflow)
1. **Access the Airflow UI**:
   - Open your browser and navigate to `http://localhost:8080`.

2. **Run the Ingestion DAG**:
   - The `data_ingestion_dag` runs automatically every minute.
   - Manually trigger the DAG if needed from the Airflow UI.

3. **Check Data Quality**:
   - Validated data is split into `good_data` and `bad_data` folders.
   - Data quality issues are logged in the database and alerts are sent via Teams.

---

### Prediction Job (Airflow)
1. **Access the Airflow UI**:
   - Open your browser and navigate to `http://localhost:8080`.

2. **Run the Prediction DAG**:
   - The `prediction_dag` runs automatically every 2 minutes.
   - Manually trigger the DAG if needed from the Airflow UI.

3. **Check Predictions**:
   - Predictions are saved to the database and can be viewed via the Streamlit UI or API.

---

### Monitoring Dashboards (Grafana)
1. **Access Grafana**:
   - Open your browser and navigate to `http://localhost:3000`.

2. **View Dashboards**:
   - **Ingested Data Monitoring**:
     - Monitor data quality issues in real-time.
   - **Data Drift and Prediction Issues**:
     - Monitor model performance and data drift.

3. **Set Up Alerts**:
   - Configure Grafana alerts for critical issues (e.g., all ingested data has errors, model predicting zero).
---
## Project Structure
    my-project/
    ├── app/
    │   ├── main.py
    │   ├── utils.py
    ├── data/
    │   ├── raw/
    │   ├── processed/
    ├── models/
    │   ├── model.pkl
    ├── tests/
    │   ├── test_main.py
    ├── requirements.txt
    ├── README.md
---
## Acknowledgments
This project would not have been possible without the help and support of the following:

- **Professors and Mentors**: Special thanks to [Professor Name] for their guidance and feedback throughout the project.
- **Open-Source Tools**: This project relies on several open-source libraries and frameworks, including:
  - [Streamlit](https://streamlit.io/) for the user interface.
  - [FastAPI](https://fastapi.tiangolo.com/) for the model API.
  - [Great Expectations](https://greatexpectations.io/) and [TensorFlow Data Validation](https://www.tensorflow.org/tfx/data_validation) for data quality validation.
  - [Grafana](https://grafana.com/) for monitoring dashboards.
- **Dataset Providers**: Thanks to [Dataset Source] for providing the dataset used in this project.
- **Community Support**: The open-source community for their invaluable resources, tutorials, and forums.

---

### Inspiration
This project was inspired by [related work, research paper, or project]. Special thanks to the authors for their groundbreaking contributions.

---

### Contributors
- [Your Name](https://github.com/your-username) - Project lead and main developer.
- [Contributor Name](https://github.com/contributor-username) - [Role or contribution].
___