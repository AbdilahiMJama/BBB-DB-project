

import re
import pandas as pd
import time

from Not_Our_Code.elis_functions import cleanEmail
from googlesearch import search


"""
For CODE REVIEWER:
We have two functions which build new urls for us
- build_url_from_email:
    - This function relies on the specific row in our csv file HAVING an EMAIL, but MISSING a URL
    - Returns the new url formed from the email, if it passes our checks
(from previous bbb team)
- get_url_from_search: 
    - makes use of a helper function (filter), which removes a site in our list if it is in our rating_sites
    - This function uses the given Company Name to search on the web for that companies website
    - 10 potential urls are extracted (excluding rating_sites) and the best match is returned
        - We may modify it if we find these urls are "bad" (aren't related to the company)
    - return the top url of choice from our algo
"""

# list of domain names we don't want
bad_domain_names = ['yahoo.com', 'gmail.com', "hotmail.com", "icloud.com", "comcast.net", "GMAIL.COM",
                    "outlook.com", "msn.com", "arvig.net", "charter.net", "winona.edu", "aol.com", "frontier.net",
                    "frontiernet.net", "results.net"]

# regex to detect valid email (works great so far, may need building upon
email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'

# list of rating sites for generating urls without emails.
rating_sites = ['mapquest', 'yelp', 'bbb', 'podium', 'porch', 'chamberofcommerce', 'angi']


def build_url_from_email(email):
    """
    Given an email, build a url from the domain name of the email.
    Only if it is not in our "bad_domain_names"
    :param email: email to build url from
    :return: Formatted url extracted from email
    """
    if re.fullmatch(email_regex, cleanEmail(email)):
        domain_name = email.split("@")[-1]
        if domain_name not in bad_domain_names:
            return "https://www.{0}/".format(domain_name)


def search_urls(df):
    """
    Given a dataframe containing rows with NO email OR website, search the web for urls for that row (business)
    :param df: list of urls
    :return: a list of new websites found via search which we should append to the dataframe OUTSIDE of the function
    """
    results = []
    rating_sites = ['mapquest', 'yelp', 'bbb', 'podium', 'porch', 'chamberofcommerce', 'angi', "yellowpages",
                    'localsolution', 'northdakota', 'allbiz', 'pitchbook', '411', 'dnd', 'thebluebook', 'opencorporates'
                    'menupix', 'buildzoom', 'buzzfile', 'manta', 'dandb', 'bloomberg', 'nextdoor', 'dnb', 'homeadvisor']

    for index, row in df.iterrows():
        business_name = df.loc[index, "BusinessName"]
        business_city = df.loc[index, 'City']
        business_id = df.loc[index, 'BusinessID']
        business_id, website = get_url_from_search(business_name, rating_sites, business_id, business_city)
        df.loc[df['BusinessID'] == business_id]['Website'] = website
        results.append(website)
        time.sleep(10)

    return df


def get_url_from_search(company_name, rating_sites, business_id, company_city_state=""):
    """
    Return company's URL given company name
    :param company_name: the name of the company
    :return: company's URL if found, else return ''
    """
    if pd.isnull(company_city_state):
        company_city_state = ""
    term = ' '.join([company_name, company_city_state])
    try:
        for j in search(term, num_results=5):
            if filter(j, rating_sites):
                print(company_name, business_id, j)
                return j
            else:
                continue
    except:
        print("Encounted Time Error")
        time.sleep(45 * 60 * 1.2)
    finally:
        print("Search completed")
        return None


def filter(url, rating_sites):
    """
    A function that checks if found url are rating sites
    Helper method to get_url_from_search
    :param url: the url
    :param rating_sites: list of any known rating sites
    :return: True if url is a not a rating site, false otherwise
    """
    domain = extract_domain_name(url)    # print("Subdomain ", sub.domain)
    for i in rating_sites:
        if domain.lower() == i:
            return False
    return True


def extract_domain_name(url):
    # Fix this, www is being returned
    # if contains www., extract word after www.
    """
    Extracts domain name from a URL and returns it as a string.
    """
    new = url.split(".")
    first = new[0]
    if first[-3:] == "www":
        first = new[1]
    if first in rating_sites:
        return
    else:
        return url