from create_urls import *
from get_status_codes import *
import pandas as pd


def main_scrape_urls(df):
    """
    Given a dataframe, adds any missing URLs found via email or web search and checks their status codes.
    If the status code is 200, updates the 'Website' column of the input dataframe with the valid URL.
    :param df: a pandas dataframe containing business information
    :return: the modified pandas dataframe
    """
    for index, row in df.iterrows():
        # Check if the row already has a valid URL
        # If it does, skip to the next row
        
        if url_exists(row):
            continue
        website = url_from_email(row)
        print("From email: ",website)

        # If a valid URL is found, add the row to the email results dataframe
        if website:
            df.loc[index, 'url'] = website
            # Otherwise, try to get a URL from the business name column using web search
        else:
            #print("Checking business name")
            df.loc[index, 'url'] = url_from_business_name(row)
    df = get_statuscode_forPandas(df)  # Check the status codes of the URLs
    # If the status code is 200, update the 'Website' column of the input dataframe with the valid URL
    # Remove this line.(figure out how to keep the processed rows.)
    df = df.loc[df['status_code'] == 200]
    return df

def url_exists(row):
    """
    helper function for main_ml
    determines if a row of data can be predicted
    """
    if _url_exists(row['url']):
        return True
    # if the url doesn't exist, return false, since we cannot predict anything
    return False


def _url_exists(url):
    """
    Checks if this row has a url
    if we have a url, return True.
    if we dont have a url, return false.
    """
    if url and not pd.isnull(url):
        return True
    else:
        return False

def url_from_email(row):
    """
    Given a row, attempts to build a URL from the email column.
    Returns the URL and BusinessID if it is valid, otherwise returns None.
    """
    if pd.isnull(row['email']):
        return None
    else:
        email = row['email']
        website = build_url_from_email(email)
        return website


def url_from_business_name(row):
    """
    Given a row, attempts to find a URL from the BusinessName column.
    Returns the URL if it is valid, otherwise returns None.
    """
    rating_sites = ['mapquest', 'yelp', 'bbb', 'podium', 'porch', 'chamberofcommerce', 'angi', "yellowpages",
                    'localsolution', 'northdakota', 'allbiz', 'pitchbook', '411', 'dnd', 'thebluebook', 'opencorporates'
                                                                                                        'menupix',
                    'buildzoom', 'buzzfile', 'manta', 'dandb', 'bloomberg', 'nextdoor', 'dnb', 'homeadvisor']
    business_name = row['company_name']
    business_id = row['firm_id']
    #company_city_state = row['PostalCode']
    if isinstance(business_name, str):
        website, business_id = get_url_from_search(business_name, rating_sites, business_id)
        if website:
            print(website)
            return website
    return None



if __name__ == '__main__':

    '''df = pd.read_csv('mn_bbb_businesses.csv')
    df, original_df = main_scrape_urls(df)  # unpack the tuple
    df.to_csv('mn_bbb_businesses_with_urls.csv', index=False)
    original_df.to_csv('mn_bbb_businesses_original.csv', index=False)'''
