"""
DISCLAIMER:This is code written by the previous teams and used by the Spring 25 team.
"""
import pandas as pd
import requests
import ThreadPoolExecutorPlus
from itertools import repeat


def get_statuscode(lst):
    """
    Gets the status code of the list of urls using threading.
    It sends a maximum of 70 (requests) threads at a time to maximize speed.

    :param lst: list of urls
    :return: a list of status codes
    """
    executor = ThreadPoolExecutorPlus.ThreadPoolExecutor(max_workers=70)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/74.0.3729.169 Safari/537.36 '
    }
    timeout = 3
    results = []
    for result in executor.map(status_code, lst, repeat(headers), repeat(timeout)):
        results.append(result)

    return results


def status_code(url, headers, timeout):
    """
    Gets a single url and returns the status code

    :param url: a single url
    :param headers: a dictionary that contains user agent strings.
    User agent string is contained in the HTTP headers and is intended to identify devices requesting online content.
    :param timeout: limits the maximum time for calling a function
    :return: status code of the url if it receives a response within the given time, if not returns -1
    """
    try:
        r = requests.get(url, verify=True, timeout=timeout, headers=headers)
        return r.status_code
    except:
        return -1


def get_statuscode_forPandas(df):
    """
    Gets the status code of the list of urls using threading.
    It sends a maximum of 70 (requests) threads at a time to maximize speed.

    :param df: dataframe
    :return: updated dataframe with status codes
    """
    urls = df['url'].values[:]
    ids = df['firm_id'].values[:]
    executor = ThreadPoolExecutorPlus.ThreadPoolExecutor(max_workers=70)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/74.0.3729.169 Safari/537.36 '
    }
    timeout = 5
    results = []
    for result in executor.map(status_code_forPandas, urls, ids, repeat(headers), repeat(timeout)):
        results.append(result)
    df['status_code'] = results
    return df

def status_code_forPandas(url, id, headers, timeout):
    """
    Gets a single url and returns the status code

    :param url: a single url
    :param headers: a dictionary that contains user agent strings.
    User agent string is contained in the HTTP headers and is intended to identify devices requesting online content.
    :param timeout: limits the maximum time for calling a function
    :return: status code of the url if it receives a response within the given time, if not returns -1
    """
    try:
        r = requests.get(url, verify=True, timeout=timeout, headers=headers)
        return r.status_code
    except:
        return -1
