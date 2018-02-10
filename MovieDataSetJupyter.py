
# coding: utf-8

# In[97]:

#importing pyspark
import pyspark
import random 
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import lit,unix_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import desc
from pyspark.sql import SparkSession
'''
Creating Session of Spark session using variable spark
'''
spark = SparkSession     .builder     .appName("Python Spark SQL basic example")     .config("spark.some.config.option", "some-value")     .getOrCreate()

'''
Setting Directory to work
'''    
set_dir="C:/shubhamprojectwork/all work 2017/Learning Projects and Material/All Big Data Stuff/Spark/sparkcasestudies/sparksql/movielensdataset/data/ml-1m/"
'''
Importing data of movies csv into df and ratings csv into dfrate
'''
df=spark.read.csv(set_dir+'movies2.csv', mode="DROPMALFORMED",inferSchema=True, header = False)
dfrate=spark.read.csv(set_dir+'ratings.csv',mode="DROPMALFORMED",inferSchema=True, header = False)
'''
Renaming Column names of imported csv as described in README file
'''
df = df.withColumnRenamed("_c0","MovieID").withColumnRenamed("_c1","Title").withColumnRenamed("_c2","Genres")
dfrate = dfrate.withColumnRenamed("_c0","UserID").withColumnRenamed("_c1","MovieID").withColumnRenamed("_c2","Rating").withColumnRenamed("_c3","Timestamp")
dfmain=df
'''
Grouping all MovieId of movies and getting count of them 
'''
Allmovid=[]

'''
Here we are only selecting movies which has rating of 5 from whole dfrate rating dataset
'''
dfrate1 = dfrate.where(dfrate['Rating'] == 5)
'''
Now we are grouping movies of 5 rating on their MovieID and generating their count 
In dfrate2 we have count of all MovieID's who have rating 5 so we have some structure like
{"MovieId":"count"}
'''
dfrate2=dfrate1.groupBy("MovieID").count()
'''
We are sorting all MovieId by their count in descending order and order is like maxcount,....,mincount and putting into dfrate3
'''
dfrate3=dfrate2.sort(desc("count")).collect()
'''
Here we are only selecting top 10 rated movies from dfrate3 into dfrate4
'''
dfrate4=dfrate3[0:9]
'''
Now we are selecting Movie Tittle on basis of top 10 movie ids
'''
SelectedmovID=[]
[SelectedmovID.append(d['MovieID']) for d in dfrate4]
AllSelectedMovTitle=[]
'''
Selecting all Movie Tittle and putting into AllSelectedMovTitle
'''
for va in SelectedmovID:
    abc=dfmain.where(dfmain['MovieID']==va)
    AllSelectedMovTitle.append(abc)


# In[98]:

'''
Showing top 10 rated Movies 
'''
for i in range(9):
    AllSelectedMovTitle[i].show()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



