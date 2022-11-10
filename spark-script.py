from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook2").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.read.connection.uri","mongodb+srv://appuser:mongo1234@cluster0.3paoz.mongodb.net/?retryWrites=true&w=majority").\
        config("spark.mongodb.write.connection.uri","mongodb+srv://appuser:mongo1234@cluster0.3paoz.mongodb.net/?retryWrites=true&w=majority").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.4").\
        getOrCreate()

#reading dataframes from MongoDB
df_transactions = spark.read.format("mongodb").option('database', 'sample_analytics').option('collection', 'transactions').load()
df_transactions.show()

df_transactions.printSchema()

first_el = df_transactions.select("transactions").collect()[0][0][0]
print(first_el)

#write into collection
new_account =  spark.createDataFrame([(999999, 99999, ['Derivatives', 'InvestmentStock', 'Brokerage', 'Commodity'])], ["account_id", "limit", "products"])
new_account.show()
new_account.write.format("mongodb").mode("append").option("database", "sample_analytics").option("collection", "accounts").save()

#Saving Dataframes to MongoDB
movAvg.write.format("mongo").option("replaceDocument", "true").mode("append").save()

#Reading Dataframes from the Aggregation Pipeline in MongoDB
pipeline2 = "[{'$match': {'account_id': 371138}}]"
aggPipelineDF = spark.read.format("mongodb").option("database", "sample_analytics").option("collection", "accounts").option("aggregation.pipeline", pipeline2).load()
aggPipelineDF.show()

"""
The following query uses the following stages:

$lookup: To do the following:
Join customers and accounts collections in the sample_analytics database based on the account ID of the customers and return the matching documents from the accounts collection in an array field named purchases.
Use $search stage in the sub-pipeline to search for customer accounts that must have purchased both CurrencyService and InvestmentStock with preference for an order limit between 5000 to 10000.

$limit: stage to limit the output to 5 results.

$project: stage to exclude the specified fields in the results.
"""

pipeline1 ="[{'$lookup':{'from':'accounts','localField':'accounts','foreignField':'account_id','as':'purchases','pipeline':[{'$search':{'compound':{'must':[{'queryString':{'defaultPath':'products','query':'products:(CurrencyServiceANDInvestmentStock)'}}],'should':[{'range':{'path':'limit','gte':5000,'lte':10000}}]}}},{'$project':{'_id':0}}]}},{'$limit':5},{'$project':{'_id':0,'address':0,'birthdate':0,'username':0,'tier_and_details':0}}]"
aggPipelineDF = spark.read.format("mongodb").option("database", "sample_analytics").option("collection", "accounts").option("aggregation.pipeline", pipeline1).load()
aggPipelineDF.show()

#using SparkSQL with MongoDB
df_accounts = spark.read.format("mongodb").option('database', 'sample_analytics').option('collection', 'accounts').load()

df_accounts.createOrReplaceTempView("acc")
sqlDF = spark.sql("SELECT * FROM acc WHERE acc.account_id=999999")

sqlDF.show()
