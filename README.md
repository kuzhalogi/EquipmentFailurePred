# Predictive Maintenance in Manufacturing

## Project Overview
Predictive Maintenance in Manufacturing aims to predict equipment failures in factories, optimize maintenance schedules, and reduce downtime. The project utilizes data analysis and machine learning techniques to achieve these objectives.

## Installation
To set up the project, follow these steps:
1. Clone the repository to your local machine: 
git clone <repository_url>

2. Navigate to the project directory:
cd <project_directory>

3. Create a python virtual environment and install the required dependencies:
`pip install -r requirements.txt`

4. Start the Airflow server and services:
`sh src/startup.sh`

5. Install the predict model package: the ML model package is in the dist directory
`pip install ./dist/equipfailpred-1.0.0.tar.gz`

6. Make import work: use this command in your terminal. Add this in the .bashrc or .zshrc file of your terminal
`export PYTHONPATH=/path/to/the/parent_folder/for/example/project-follow-up/:$PYTHONPATH`

7. Set up postgresql: follow the commands in the psql.sql inside the database directory.
`sudo -i -u postgres`
`\psql -d database_name`
8. Start API: in a separate terminal move to the API directory. then execute this command:
`uvicorn main:app --reload`
this command will start the api, and give the url like http://127.0.0.1:8000
9.Start the webapp: in a separate terminal move to the webapp directory. then execute this command, which is the starting point of the streamlit application.
`streamlit run predapp.py`
10. Finished setup: now you are all set, you can predict the equipment failure with this app, by providing the csv file or inputs manually.

## Usage
Once the project is set up, you can use it as follows:
1. Data Splitting and Quality Check:
- Run the ETL pipeline DAG (`etl.py`) in Airflow to split and perform quality checks on the data.
2. Analyze Data:
- Explore the data in the `data` directory, including raw, good, and bad data files.

## Files and Directories
- `dags/etl.py`: Airflow DAG for data splitting and quality check.
- `src/csvsplitscript.py`: Python script for introducing data issues and splitting data.
- `src/docker-compose.yml`: Docker Compose configuration for setting up Airflow and PostgreSQL services.
- `src/startup.sh`: Shell script for starting Airflow services.
- `src/stop.sh`: Shell script for stopping Airflow services.

## Credits
- Dataset Owner: SHIVAM BANSAL
-Project Team Members:
•	Modupe Agnes O’tayo
•	Kuzhalogi Murthy
•	Yingdi Su
•	Ravi Shankar Purushothaman
•	Sethupathi Subramanis

Feel free to customize it further based on your specific project details and preferences.

## Exploratory Data Analysis (EDA)

RangeIndex: 10000 entries, 0 to 9999
Data columns (total 10 columns):

| Column                   | Non-Null Count | Dtype   |
|--------------------------|----------------|---------|
| UDI                      | 10000 non-null | int64   |
| Product ID               | 10000 non-null | object  |
| Type                     | 10000 non-null | object  |
| Air temperature [K]      | 10000 non-null | float64 |
| Process temperature [K]  | 10000 non-null | float64 |
| Rotational speed [rpm]   | 10000 non-null | int64   |
| Torque [Nm]              | 10000 non-null | float64 |
| Tool wear [min]          | 10000 non-null | int64   |
| Target                   | 10000 non-null | int64   |
| Failure Type             | 10000 non-null | object  |

dtypes: float64(3), int64(4), object(3)

## Features
-- {'Air temperature [K]', 'Process temperature [K]', 'Rotational speed [rpm]', 'Torque [Nm]', 'Tool wear [min]', 'Type'}

## Label
-- {'Target'} (have 1 as failure and zeros as not failure) -> need to predict failures.

## Model
The prediction is classifier, so random forest model is used to predict the target(failures) also for simplicity.

Create modules packages to use in our project
-- python setup.py sdist Inside the setup.py file:
from setuptools import setup, find_packages

setup(
    name='mymodule',
    version='1.0.0',
    packages=find_packages(),
)

This will create a python zip for windows or tar.gz file for unix, we can install this dist using pip.
`pip install mymodule-1.0.0.tar.gz`

Modify the version number if you made or put any changes in the module.
`pip install /path/to/your_package.tar.gz --upgrade`
