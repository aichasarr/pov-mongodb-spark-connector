# MongoDB Spark Connector

__Ability to integrate the database with Spark using a native Spark Connector provided by the database supplier.__

The official MongoDB Connector for Apache Spark is developed and supported by MongoDB engineers. The Connector Makes data stored in MongoDB available to Spark and gives you have access to all Spark libraries for use with MongoDB data. You can write and read data in MongoDB from Spark, even run aggregation pipelines.

More on why MongoDB & Spark go hand-in-hand: [MongoDB Connector for Apache Spark](https://www.mongodb.com/products/spark-connector).

__SA Maintainer__: [Aicha Sarr](mailto:aicha.sarr@mongodb.com) <br/>
__Time to setup__: 45 mins <br/>
__Time to execute__: 20 mins <br/>

---
## Description
This proof showcases how to leverage MongoDB data in your JupyterLab notebooks via the MongoDB Spark Connector and PySpark. We will load financial transactions data into MongoDB, write and read from collections then compute an aggregation pipeline running *$lookup* query with *$search* to find customers from the customers's collection whose accounts have purchased both *CurrencyService* and *InvestmentStock* products in the accounts collection.

This repository has two components:
- Docker compose file which contains the docker compose file that to spin up the environment
- Pyspark file which contains the scripts to execute into JupyterLab
<!--
- Data folder which contains the data and command to execute to upload the data into an Atlas database
-->

The Docker file will spin up the following environment:

![Image of docker environment](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/diagram.png)

The database *sample_analytics* made available is provided with the Atlas sample dataset and contains three collections for a typical financial services application:
- accounts: contains details on customer accounts.
- customers: contains details on customers including what accounts they hold.
- transactions: contains contains transactions details for customers.


----
## Setup
__1. Configure Laptop__

* Ensure Docker is installed. Here is a [link to the download](https://docs.docker.com/desktop/) page.
We will use version 10.0 of the Spark Connector which is compatible with Spark version 3.1 or later and MongoDB version 6.0 or later.

__2. Configure Atlas Environment__

* Log-on to your [Atlas account](http://cloud.mongodb.com/) (using the MongoDB SA preallocated Atlas credits system) and navigate to your SA project

* In the project's Security tab, choose to add a new user, and for User Privileges specify Atlas admin (make a note of the password you specify)

* Also in the Security tab, add a new IP Whitelist for your laptop's current IP address

* Create an M10 based 3 node replica-set in an AWS region of your choice, running MongoDB version 6.0 (To run $lookup query with $search, your cluster must run MongoDB v6.0 or higher. It is available on all cluster sizes.)

* Once the cluster has been fully provisioned, in the Atlas console, click the ... (ellipsis) for the cluster, select Load Sample Dataset. In the modal dialog, confirm that you want to load the sample dataset by choosing Load Sample Dataset

* In the Atlas console, once the dataset has fully loaded, click the Collections button for the cluster, and navigate to the sample_analytics.accounts collection. Under the Search tab, choose to Create Search Index

![Image of creating a search index](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/createsearchindex1.png)

* To select the account collection as a data source, type analytics in the text box or click sample_analytics as shown below

![Image of creating a search index](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/createsearchindex2.png)

* Keep all of the default options and select Create Index

* The default index structure can be edited as shown below

![Image of creating a search index](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/createsearchindex3.png)

* The index creation process should take approximately 4 minutes.

![Image of creating a search index](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/createsearchindex4.png)


* __ Getting the environment up and running__

Execute the `run.sh` script file, docker daemon need to be running. For a first run, it might take up to 15 minutes to complete.
This runs the docker compose file which deploy a Spark environment with a master node located at port 8080 and two worker nodes listening on ports 8081 and 8082 respectively. The Atlas cluster will be used for both reading data into Spark and writing data from Spark back into Atlas.

Note: You may have to mark the .sh file as runnable with the `chmod` command i.e. `chmod +x run.sh`

**If you are using Windows, launch a PowerShell command window and run the `run.ps1` script instead of run.sh.**

To verify our Spark master and works are online navigate to http://localhost:8080

![Image of spark cluster](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/sparkcluster.png)

The Jupyter notebook URL which includes its access token will be listed at the end of the script.
NOTE: This token will be generated when you run the docker image so it will be different for you. You will need to replace the characters generated after *http://* with *127.0.0.1* .
Here is what it looks like:
http://127.0.0.1:8888/?token=b6ee384b17eb11ad3cabf133aaa052aa2512c6b89c583502

*Ps: If you launch the containers outside of the script, you can still get the URL by issuing the following command:*

`docker exec -it jupyterlab  /opt/conda/bin/jupyter server list`

![Image of url with token](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/url_jupyterlab.png)

To use MongoDB data with Spark, create a new Python Jupyter notebook by navigating to the Jupyter URL and under notebook select Python3:

![Image of New Python notebook](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/newpythonnotebook.png)


---
## Execution

Now You can run through the following demo script.  You can copy and execute one or more of these lines :

* To start, you will create the SparkSession and set the environment to use our Atlas cluster.

In the Atlas console, for the database cluster you deployed, click the Connect button, select Connect your application, choose Python Driver and copy the Connection String.

In the SparkSession variable, for the read and write config parameters, paste the connection string you just copied and fill in your username and password.

To use the Spark Connector, we will use the [Maven dependency](https://search.maven.org/artifact/org.mongodb.spark/mongo-spark-connector/10.0.5/jar) and set the config parameter to this:

`config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.5")`

You can also download the connector and add it Jupyterlab. If you want to use this way, you will need to update the config parameter to this:

`config("spark.jars", "path_to_the_connector")`


```
from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.read.connection.uri","[your_connection_string]").\
        config("spark.mongodb.write.connection.uri","[your_connection_string]").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.5").\
        getOrCreate()
```
![Image of PySpark1](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark1.png)

* Next, load a dataframes from transactions collection

```
df_transactions = spark.read.format("mongodb").option('database', 'sample_analytics').option('collection', 'transactions').load()
df_transactions.show()
```

![Image of PySpark2](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark2.png)

* Let’s verify the data was loaded by looking at the schema:

```
df_transactions.printSchema()
```

![Image of PySpark3](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark3.png)

* We can now write data into accounts collection by creating account in a dataframe and inserting into the collection

```

new_account =  spark.createDataFrame([(999999, 99999, ['Derivatives', 'InvestmentStock', 'Brokerage', 'Commodity'])], ["account_id", "limit", "products"])
new_account.write.format("mongodb").mode("append").option("database", "sample_analytics").option("collection", "accounts").save()
```

![Image of PySpark4](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark4.png)

You can issue a find request on the newly created account_id to check if it has been well inserted.

![Image of PySpark8](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark8.png)


* We can also use the power of the MongoDB Aggregation Framework to pre-filter, sort or aggregate our MongoDB data.
For a simple aggregation pipepile, let's retrieve accounts where account_id equals to 999999

```
pipeline1 = "[{'$match': {'account_id': 999999}}]"
aggPipelineDF = spark.read.format("mongodb").option("database", "sample_analytics").option("collection", "accounts").option("aggregation.pipeline", pipeline1).load()
aggPipelineDF.show()
```

![Image of PySpark5](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark5.png)

* Then lets find customers from the customers collections whose accounts have purchased both *CurrencyService* and *InvestmentStock* products in the accounts collection.

The following query uses these stages:

- $lookup: join customers and accounts collections in the sample_analytics database based on the account ID of the customers and return the matching documents from the accounts collection in an array field named purchases. Use $search stage in the sub-pipeline to search for customer accounts that must have purchased both CurrencyService and InvestmentStock with preference for an order limit between 5000 to 10000.
- $limit: stage to limit the output to 5 results.
- $project: stage to exclude the specified fields in the results.

```
pipeline2 = """[
    {
        '$lookup': {
            'from': 'accounts',
            'localField': 'accounts',
            'foreignField': 'account_id',
            'as': 'purchases',
            'pipeline': [
                {
                    '$search': {
                        'compound': {
                            'must': [
                                {
                                    'queryString': {
                                        'defaultPath': 'products',
                                        'query': 'products: (CurrencyService AND InvestmentStock)'
                                    }
                                }
                            ],
                            'should': [
                                {
                                    'range': {
                                        'path': 'limit',
                                        'gte': 5000,
                                        'lte': 10000
                                    }
                                }
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0
                    }
                }
            ]
        }
    }, {
        '$limit': 5
    }, {
        '$project': {
            '_id': 0,
            'address': 0,
            'birthdate': 0,
            'username': 0,
            'tier_and_details': 0
        }
    }
]"""

aggPipelineDF = spark.read.format("mongodb").option("database", "sample_analytics").option("collection", "accounts").option("aggregation.pipeline", pipeline2).load()
aggPipelineDF.show()
```

![Image of PySpark6](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark6.png)

* Finally we can use SparkSQL to issue ANSI-compliant SQL against MongoDB data as follows:

```
df_accounts = spark.read.format("mongodb").option('database', 'sample_analytics').option('collection', 'accounts').load()
df_accounts.createOrReplaceTempView("acc")
sqlDF = spark.sql("SELECT * FROM acc WHERE acc.account_id=999999")
sqlDF.show()
```

![Image of PySpark7](https://github.com/aichasarr/pov-mongodb-spark-connector/blob/main/images/pyspark7.png)

---
## Measurement

This proof should have demonstrated how to use the MongoDB Spark Connector and some capabilities :

* Creation of a JupyterLab notebook and loading MongoDB data
* Update of the collection with the new data
* Computation of an aggregation pipeline with $lookup and $search operators
* Computation of SQL against MongoDB Data

This proof shows how easy it is to integrate MongoDB data within your Spark application.  For more information on the Spark Connector check out the [online documentation](https://docs.mongodb.com/spark-connector/master/).
