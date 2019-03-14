""" Read in json file, resample to smaller size, and write out
The output file will be in a directory called "downsized.json".
The file itself will be in that directory, and named "part-00000".
Can be renamed to whatever you want, e.g. HNStories-smaller.json, and Gzipped.
"""

import json
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime as dt
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
print(sc)
print("Ready to go!")
%matplotlib inline

sc  # show SparkContext, version etc.

# read in the original file, which had 1,333,333 json entries
dataset_json = sc.textFile("data/HNStories.json.gz")  # too large for github
print("dataset_json count {:,}".format(dataset_json.count()))  #how large?
data_downsized = dataset_json.sample(withReplacement=False,  # returns an RDD
                                fraction=0.74, seed=42)  # reduce to 74% size
type(data_downsized)  # check it returns an RDD :-)
data_downsized.saveAsTextFile("downsized.json")  # save reduced size file