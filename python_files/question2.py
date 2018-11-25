#!/usr/bin/env python
# coding: utf-8

# In[12]:


# importing the classes and functions
# importing the classes and functions
from pyspark import *
from pyspark.conf import *
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree

# setting the string variables
app_name = 'Glass Data'
master = 'local'

# setting the data file for glassdat
glassdata_path = 'glass.data'

# In[2]:


# configuring the Spark and setting the master & app name
spark = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=spark)


# In[3]:


def training_label_mapper(line):
    temp = line.split(',')
    n = len(temp)
    for i in range(n - 1):
        temp[i] = float(temp[i])
    temp[n - 1] = float(temp[n - 1]) - 1
    return LabeledPoint(temp[n - 1], temp[:n - 1])


def testing_feature_mapper(line):
    temp = line.split(',')
    n = len(temp)
    for i in range(n - 1):
        temp[i] = float(temp[i])
    temp[n - 1] = float(temp[n - 1]) - 1
    return LabeledPoint(temp[n - 1], temp[:n - 1])


# In[4]:


glassdata_rdd = sc.textFile(glassdata_path)
training, testing = glassdata_rdd.randomSplit(weights=[0.6, 0.4], seed=1)

training_data = training.map(training_label_mapper)
testing_data = testing.map(testing_feature_mapper)

# In[5]:


decision_tree_model = DecisionTree.trainClassifier(training_data, 7, {})
print(decision_tree_model.toDebugString())

# In[6]:


predictions_rdd = decision_tree_model.predict(testing_data.map(lambda x: x.features))

# In[25]:


prep_data = predictions_rdd.zip(testing_data.map(lambda x: x.label))
metrics = MulticlassMetrics(prep_data)
accuracy = metrics.accuracy * 100

print("Accuracy of the model is " + str(accuracy) + "%.")
