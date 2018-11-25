#!/usr/bin/env python
# coding: utf-8

# In[35]:


from pyspark import SparkConf, SparkContext
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.recommendation import ALS, Rating

# In[2]:


# setting data path and app name
ratings_path = 'ratings.data'
app_name = 'Ratings ALS'
master = 'local'

# In[3]:


# configuring the Spark and setting the master & app name
spark = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=spark)


# In[4]:


def parse_rating_mapper(line):
    temp = line.split('::')
    return Rating(int(temp[0]), int(temp[1]), float(temp[2]))


# In[5]:


ratings = sc.textFile(ratings_path).map(parse_rating_mapper)
training, testing = ratings.randomSplit(weights=[0.6, 0.4])

# In[50]:


als_model = ALS.train(training, rank=2)

# In[51]:


testing_data = testing.map(lambda x: (x[0], x[1]))
predictions_data = als_model.predictAll(testing_data).map(lambda x: ((x[0], x[1]), x[2]))
ratings_predictions_data = predictions_data.join(testing.map(lambda r: ((r[0], r[1]), r[2])))

# In[52]:


prep_data = ratings_predictions_data.map(lambda r: r[1])
metrics = RegressionMetrics(prep_data)
rmse = metrics.rootMeanSquaredError

# In[45]:


rmse  # for rank=5

# In[49]:


rmse  # for rank=1

# In[53]:


rmse  # for rank=2

# In[ ]:
