from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook2").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.read.connection.uri","mongodb+srv://[your_username]:[your_password]@[your_connection_string]").\
        config("spark.mongodb.write.connection.uri","mongodb+srv://[your_username]:[your_password]@[your_connection_string]").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.5").\
        getOrCreate()

#reading dataframe from transactions collection
df_transactions = spark.read.format("mongodb").option('database', 'sample_analytics').option('collection', 'transactions').load()
df_transactions.show()

df_transactions.printSchema()

first_el = df_transactions.select("transactions").collect()[0][0][0]
print(first_el)

#write dataframe into accounts collection
new_account =  spark.createDataFrame([(999999, 99999, ['Derivatives', 'InvestmentStock', 'Brokerage', 'Commodity'])], ["account_id", "limit", "products"])
new_account.show()
new_account.write.format("mongodb").mode("append").option("database", "sample_analytics").option("collection", "accounts").save()

#Reading Dataframes from the Aggregation Pipeline in MongoDB
pipeline1 = "[{'$match': {'account_id': 999999}}]"
aggPipelineDF = spark.read.format("mongodb").option("database", "sample_analytics").option("collection", "accounts").option("aggregation.pipeline", pipeline1).load()
aggPipelineDF.show()


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

#using SparkSQL with MongoDB
df_accounts = spark.read.format("mongodb").option('database', 'sample_analytics').option('collection', 'accounts').load()

df_accounts.createOrReplaceTempView("acc")
sqlDF = spark.sql("SELECT * FROM acc WHERE acc.account_id=999999")

sqlDF.show()
