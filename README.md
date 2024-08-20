## ETL Demo with Apache Airflow



This repository showcases an Extract, Transform, and Load (ETL) process using [Apache Airflow ](https://airflow.apache.org/).

**Process Overview:**

  The Airflow DAG schedules jobs to perform these tasks for UCI Rankings data:

*  **Extract:** Data is retrieved from a website (for demo purposes only).
*  **Transform:** The data is processed using the Beautiful Soup library.
*  **Load:** The transformed data is loaded into a MySQL database.

**Note:** The DAG will fail if the source dataset for the previous ISO week number is empty.

**Data Storage:**
* Data is stored at each step (Extract, Transform, Load) within a Data Lake (here, Redis).
* This facilitates access to the full dataset if issues like format changes arise.

**Running the Demo:**

1.  **Clone the repository.**
2.  **Run `docker compose up`** .
* Airflow may take some time to load and generate warnings/messages. This is normal.
3.  **Create Connections:**
* You can either use the Airflow UI or the provided `connections.json` file with `airflow connections import connections.json`. This establishes connections for MySQL and Redis.
4.  **Set Variables:**
* Use the Airflow UI or the provided `variables.json` file with `airflow variables import variables.json`. This sets the source website URL.
5.  **Database Setup:**
* Use the provided database shell script to create the database.
6.  **Enable DAG:**
* Access the Airflow UI and enable the DAG. It's scheduled to run weekly, however actual source data update schedule is not currently known. You can also trigger manual runs.
7.  **Default Login:**
* The UI can be accessed at http://0.0.0.0:8080
*  `airflow / airflow`

### Possible Enhancements

* Add a callback to the DAG for failures, to notify in some manner (Slack Hook, email, etc.)
* Add parameters to the DAG to pass a season and week number to parse specific ranking sets.

### TODO

* Add Unit Tests
* Add Docstrings to code base
* Add pre-commit hooks

**Note:** Extracting data directly from websites for production use is not recommended. This is a demonstration only.
