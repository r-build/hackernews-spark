""" Spark data exploration of HackerNews posts.

Assumes:
 - a spark context established, e.g.:
    sc = SparkContext.getOrCreate()
 - a json file of HackerNews posts, with one JSON entry per line. 
 - the data file is read in, each line is turned into a dictionary in the
 resulting RDD.
    dataset_json = sc.textFile(\"data/HNStories-smaller.json.gz\")
    dataset = dataset_json.map(lambda x: json.loads(x))
    dataset.persist()  #  cache the RDD
"""


import json
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime as dt
from pyspark import SparkContext
from datetime import datetime as dt
import re


def get_hour(rec):
    """
    Given a record, return hour from creation timestamp
    """
    ttt = dt.utcfromtimestamp(rec['created_at_i'])
    return ttt.hour


def get_words(line):
    """
    Given a string, return the words in a list
    """
    return re.compile(r'\w+').findall(line)


def count_elements_in_dataset(dataset):
    """
    Given a dataset loaded on Spark, return the
    number of elements.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: number of elements in the RDD
    """

    return dataset.count()


def get_first_element(dataset):
    """
    Given a dataset loaded on Spark, return the
    first element
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: the first element of the RDD
    """

    return dataset.first()


def get_all_attributes(dataset):
    """
    Each element is a dictionary of attributes and their values for a
    post.
    Can you find the set of all attributes used throughout the RDD?
    The function dictionary.keys() gives you the list of attributes of a
    dictionary.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: all unique attributes collected in a list
    """

    # one of my favourite solutions
    key_set = dataset.map(set).reduce(lambda x, y: x.union(y))
    key_list = list(key_set)
    return key_list


def get_elements_w_same_attributes(dataset):
    """
    We see that there are more attributes than just the one used in the first
    element.
    This function should return all elements that have the same attributes
    as the first element.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD containing only elements with same attributes as the
    first element
    """

    def compare_keys(elt):
        return set(elt) == first_set

    first_set = set(dataset.first())
    f_set = dataset.filter(compare_keys)
    return f_set


def get_min_max_timestamps(dataset):
    """
    Find the minimum and maximum timestamp in the dataset
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: min and max timestamp in a tuple object
    :rtype: tuple
    """

    def extract_time(timestamp):
        return dt.utcfromtimestamp(timestamp)

    min_timestamp = dataset.map(lambda x: x['created_at_i']).min()
    max_timestamp = dataset.map(lambda x: x['created_at_i']).max()
    return (extract_time(min_timestamp), extract_time(max_timestamp))


def get_number_of_posts_per_bucket(dataset, min_time, max_time):
    """
    Using the `get_bucket` function defined in the notebook (redefine it in
    this file), this function should return a
    new RDD that contains the number of elements that fall within each bucket.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :param min_time: Minimum time to consider for buckets (datetime format)
    :param max_time: Maximum time to consider for buckets (datetime format)
    :return: an RDD with number of elements per bucket
    """

    def get_bucket(rec, min_t, max_t):
        interval = (max_t - min_t + 1) / 200.0
        return int((rec['created_at_i'] - min_t)/interval)

    min_ts = dt.timestamp(min_time)
    max_ts = dt.timestamp(max_time)
    half_buckets_rdd = dataset.map(lambda x: (
        get_bucket(x, min_ts, max_ts), 1)).groupByKey()
    buckets_rdd = half_buckets_rdd.map(lambda pair: (pair[0], len(pair[1])))
    return buckets_rdd


def get_number_of_posts_per_hour(dataset):
    """
    Using the `get_hour` function defined in the notebook (redefine it in this
    file), this function should return a
    new RDD that contains the number of elements per hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with number of elements per hour
    """

    half_hours_buckets_rdd = dataset.map(
        lambda x: (get_hour(x), 1)).groupByKey()
    hours_buckets_rdd = half_hours_buckets_rdd.map(
        lambda pair: (pair[0], len(pair[1])))
    return hours_buckets_rdd


def get_score_per_hour(dataset):
    """
    The number of points scored by a post is under the attribute `points`.
    Use it to compute the average score received by submissions for each hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with average score per hour
    """

    half_scores_p_hr_rdd = dataset.map(
        lambda x: (get_hour(x), x['points'])).groupByKey()
    scores_per_hour_rdd = half_scores_p_hr_rdd.map(
        lambda pair: (pair[0], sum(pair[1])/len(pair[1])))
    return scores_per_hour_rdd


def get_proportion_of_scores(dataset):
    """
    It may be more useful to look at sucessful posts that get over 200 points.
    Find the proportion of posts that get above 200 points per hour.
    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of scores over 200 per hour
    """

    half_prop_scores_rdd = dataset.map(
        lambda x: (get_hour(x), x['points'])).groupByKey()
    prop_per_hour_rdd = half_prop_scores_rdd.map(
        lambda pair: (pair[0], sum(i > 200 for i in pair[1])/len(pair[1])))
    return prop_per_hour_rdd


def get_proportion_of_success(dataset):
    """
    Using the `get_words` function defined in the notebook to count the
    number of words in the title of each post, look at the proportion
    of successful posts for each title length.

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the proportion of successful post per title length
    """

    half_title_rdd = dataset.map(
        lambda x: (len(get_words(x['title'])) if 'title' in x else 0,
                   x['points'])).groupByKey()
    prop_per_title_length_rdd = half_title_rdd.map(
        lambda pair: (pair[0], sum(i > 200 for i in pair[1])/len(pair[1])))
    return prop_per_title_length_rdd


def get_title_length_distribution(dataset):
    """
    Count for each title length the number of submissions with that length.

    Note: If an entry in the dataset does not have a title, it should
    be counted as a length of 0.

    :param dataset: dataset loaded in Spark context
    :type dataset: a Spark RDD
    :return: an RDD with the number of submissions per title length
    """

    half_subs_rdd = dataset.map(
        lambda x: (len(get_words(x['title'])) if 'title' in x else 0,
                   1)).groupByKey()
    submissions_per_length_rdd = half_subs_rdd.map(
        lambda pair: (pair[0], sum(pair[1])))
    return submissions_per_length_rdd
