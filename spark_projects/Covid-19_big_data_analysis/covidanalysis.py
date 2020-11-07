from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext
from random import randint
from time import sleep
from pyspark.sql.session import SparkSession
import logging
import sys
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import column
from pyspark.sql.functions import col
from hdfs import Config
import os, math
import subprocess
from pyspark.sql.functions import lit
from pyspark.sql import Row

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
logger.addHandler(ch)


import sys
def dataprocessing(filePath, sqlContext):
    logger.info( "Entering into Function")
    df_covid = sqlContext.read.format("csv").option("header",'true').load(filePath)
    df_covid=df_covid.withColumn("month",F.when(F.col("month")==12,'December').otherwise(F.col("month")))
    df_covid=df_covid.withColumn("month",F.when(F.col("month")==1,'January').otherwise(F.col("month")))
    df_covid=df_covid.withColumn("month",F.when(F.col("month")==2,'February').otherwise(F.col("month")))
    df_covid=df_covid.withColumn("month",F.when(F.col("month")==3,'March').otherwise(F.col("month")))
    df_covid=df_covid.withColumn("month",F.when(F.col("month")==4,'April').otherwise(F.col("month")))


    logger.info( '#############################DataSet has:%s' , df_covid.count())
    sleepInterval = randint(10,100)
    logger.info( '#############################Sleeping for %s' , sleepInterval)
    sleep(sleepInterval)
    df_covid.createOrReplaceTempView("covid")
    sqlDF = sqlContext.sql("SELECT month as Month, year as Year, countriesAndTerritories as CountryCode, (sum(cases) / sum(TestPerformed)) *100 as InfectionRate, (sum (deaths) / sum (cases) )*100 as DeathRate FROM covid group by month, year, countriesAndTerritories order by countriesAndTerritories,month,year")
    sqlDF.coalesce(1).write.mode("overwrite").save(path="hdfs://bdrenfdludcf01:9000/OUTPUT/result.csv",format='csv',sep=';')
    #sqlDF.filter(sqlDF["countriesAndTerritories"] == "Bangladesh").show()
if __name__ == '__main__':

    print(sys.argv)
    filename = "Covid_Analysis_DataSet.csv"

    logger.info('----------------------')
    logger.info('Filename:%s', filename)
    #logger.info('Iterations:%s', iterations )
    #logger.info('SparkMaster:%s', sparkHost)

    #logger.info('----------------------')
    basePath = 'hdfs://bdrenfdludcf01:9000/ASSIGNMENT/'
    absafilePath = basePath + filename
    logger.info( '........Starting spark..........Loading from %s ' , filename)
    logger.info( 'Starting up....')
    # Configure Spark
    conf = SparkConf().setAppName("Covid_Analyzer")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    dataprocessing(absafilePath, sqlContext)
