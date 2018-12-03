#!/usr/bin/env python
# coding: utf-8

# In[1]:


#importing the classes and functions
from pyspark import *
from pyspark.conf import *
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.classification import NaiveBayes

#setting the string variables
app_name = 'Glass Data'
master = 'local'

#setting the data file for glassdat
glassdata_path = 'glass.data'


# In[2]:


#configuring the Spark and setting the master & app name
spark = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=spark)


# In[3]:


def data_mapper(line):
    temp = line.split(',')
    n = len(temp)
    for i in range(n - 1):
        temp[i] = float(temp[i])
    temp[n - 1] = float(temp[n - 1]) - 1
    return LabeledPoint(temp[n - 1], temp[:n - 1])


# In[4]:


glassdata_rdd = sc.textFile(glassdata_path).map(data_mapper)
training, testing = glassdata_rdd.randomSplit(weights=[0.6, 0.4])


# In[5]:


decision_tree_model = DecisionTree.trainClassifier(training, 7, {})
print(decision_tree_model.toDebugString())


# In[6]:


decision_tree_predictions_rdd = decision_tree_model.predict(testing.map(lambda x: x.features))


# In[7]:


dt_prep_data = decision_tree_predictions_rdd.zip(testing.map(lambda x: x.label))
dt_metrics = MulticlassMetrics(dt_prep_data)
dt_accuracy = dt_metrics.accuracy * 100

print("Accuracy of the model is " + str(dt_accuracy) + "%.")


# In[8]:


naive_bayes_model = NaiveBayes.train(training)


# In[9]:


naive_bayes_predictions_labels = testing.map(lambda p: (naive_bayes_model.predict(p.features), p.label))
accuracy = 1.0 * naive_bayes_predictions_labels.filter(lambda pl: pl[0] == pl[1]).count() / testing.count()
print('Accuracy of the model is {}%.'.format(accuracy*100))


# In[ ]:




