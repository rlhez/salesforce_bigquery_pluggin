This Airflow DAG + Operators aims at:

- Pulling data from Salesforce (you can select the objects)
- Sending this data to a BigQuery Dashboard
- Both things while making sure the schema is identical

Aside from this main topic, we wanted:

- To keep a daily history of Salesforce changes (that's handled by using partitioned tables)