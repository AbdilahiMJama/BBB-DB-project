# -*- coding: utf-8 -*-
"""
Created on Fri Jan 27 12:11:35 2023

@author: Eli Johnson
"""

import re, tldextract

# Incomplete, but sufficient for email/domain checking
topLevelDomains = ['com', 'org', 'net', 'int', 'edu', 'gov', 'mil', 'info', 'top', 'xyz']
# Country codes
topLevelDomains += ['ac', 'ad', 'ae', 'af', 'ag', 'ai', 'al', 'am', 'ao', 'aq', 'ar', 'as', 'at', 'au', 'aw',
                    'ax', 'az', 'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'bj', 'bm', 'bn', 'bo', 'bq',
                    'br', 'bs', 'bt', 'bw', 'by', 'bz', 'ca', 'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck', 'cl',
                    'cm', 'cn', 'co', 'cr', 'cu', 'cv', 'cw', 'cx', 'cy', 'cz', 'de', 'dj', 'dk', 'dm', 'do',
                    'dz', 'ec', 'ee', 'eg', 'eh', 'er', 'es', 'et', 'eu', 'fi', 'fj', 'fk', 'fm', 'fo', 'fr',
                    'ga', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi', 'gl', 'gm', 'gn', 'gp', 'gq', 'gr', 'gs', 'gt',
                    'gu', 'gw', 'gy', 'hk', 'hm', 'hn', 'hr', 'ht', 'hu', 'id', 'ie', 'il', 'im', 'in', 'io',
                    'iq', 'ir', 'is', 'it', 'je', 'jm', 'jo', 'jp', 'ke', 'kg', 'kh', 'ki', 'km', 'kn', 'kp',
                    'kr', 'kw', 'ky', 'kz', 'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu', 'lv', 'ly',
                    'ma', 'mc', 'md', 'me', 'mg', 'mh', 'mk', 'ml', 'mm', 'mn', 'mo', 'mp', 'mq', 'mr', 'ms',
                    'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz', 'na', 'nc', 'ne', 'nf', 'ng', 'ni', 'nl', 'no',
                    'np', 'nr', 'nu', 'nz', 'om', 'pa', 'pe', 'pf', 'pg', 'ph', 'pk', 'pl', 'pm', 'pn', 'pr',
                    'ps', 'pt', 'pw', 'py', 'qa', 're', 'ro', 'rs', 'ru', 'rw', 'sa', 'sb', 'sc', 'sd', 'se',
                    'sg', 'sh', 'si', 'sk', 'sl', 'sm', 'sn', 'so', 'sr', 'ss', 'st', 'su', 'sv', 'sx', 'sy',
                    'sz', 'tc', 'td', 'tf', 'tg', 'th', 'tj', 'tk', 'tl', 'tm', 'tn', 'to', 'tr', 'tt', 'tv',
                    'tw', 'tz', 'ua', 'ug', 'uk', 'us', 'uy', 'uz', 'va', 'vc', 've', 'vg', 'vi', 'vn', 'vu',
                    'wf', 'ws', 'ye', 'yt', 'za', 'zm', 'zw']

stateProvinceToAbbrev = {'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'american samoa': 'AS',
                         # US states and territories
                         'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
                         'district of columbia': 'DC',
                         'florida': 'FL', 'georgia': 'GA', 'guam': 'GU', 'hawaii': 'HI', 'idaho': 'ID',
                         'illinois': 'IL',
                         'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA',
                         'maine': 'ME',
                         'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
                         'mississippi': 'MS',
                         'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH',
                         'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC',
                         'north dakota': 'ND',
                         'northern mariana islands': 'CM', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR',
                         'pennsylvania': 'PA',
                         'puerto rico': 'PR', 'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD',
                         'tennessee': 'TN',
                         'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'virgin islands': 'VI',
                         'washington': 'WA',
                         'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
                         'newfoundland': 'NL', 'labrador': 'NL', 'newfoundland and labrador': 'NL', 'nova scotia': 'NS',
                         # Canadian provinces and territories
                         'prince edward island': 'PE', 'new brunswick': 'NB', 'quebec': 'QC', 'ontario': 'ON',
                         'manitoba': 'MB',
                         'saskatchewan': 'SK', 'alberta': 'AB', 'british columbia': 'BC', 'nunavut': 'NU',
                         'northwest territories': 'NT', 'the northwest territories': 'NT', 'yukon territory': 'YT'}

# used in function cheapValidateUSZip()
zipValidation = {'AK': '99', 'AL': '3(5|6)', 'AR': '7(1|2|5502)', 'AZ': '8(5|6)', 'CA': '9[0-6]', 'CO': '8(0|1)',
                 'CT': '06',  # US states and territories
                 'DC': '20', 'DE': '19', 'FL': '3[2-4]', 'GA': '3(0|1|9(8|901))', 'HI': '96(7|8)',
                 'IA': '(5[0-2]|681(19|20))',
                 'ID': '83', 'IL': '6[0-2]', 'IN': '4(6|7)', 'KS': '6(6|7)', 'KY': '4[0-2]', 'LA': '7(0|1)',
                 'MA': '0(1|2|5)',
                 'MD': '2(0|1)', 'ME': '0(3|4)', 'MI': '4(8|9)', 'MN': '5(5|6)', 'MO': '6[3-5]', 'MS': '(3(8|9)|71233)',
                 'MT': '59', 'NC': '2(7|8)', 'ND': '58', 'NE': '6(8|9)', 'NH': '03', 'NJ': '0(7|8)', 'NM': '8(7|8)',
                 'NV': '8(8|9)',
                 'NY': '(1[0-4]|06390)', 'OH': '4[3-5]', 'OK': '7(3|4)', 'OR': '97', 'PA': '1[5-9]', 'RI': '02(8|9)',
                 'SC': '29',
                 'SD': '57', 'TN': '3(7|8)', 'TX': '(7[5-9]|885|73301)', 'UT': '84', 'VA': '2([2-4]|0(0|1))',
                 'VT': '05',
                 'WA': '9(8|9)', 'WI': '5(3|4)', 'WV': '2[4-6]', 'WY': '8(2|3)',
                 'NL': 'A', 'NS': 'B', 'PE': 'C', 'NB': 'E', 'QC': '[GHJ]', 'ON': '[KLMNP]', 'MB': 'R', 'SK': 'S',
                 'AB': 'T', 'BC': 'V',  # Canadian provinces and territories
                 'NU': 'X', 'NT': 'X', 'YT': 'Y'}


# I use this for numeric strings with leading zeros (e.g. zip, BBBID)
# since writing to certain formats will often erase these
def addLeadingZeros(string, width=4):
    n = max(0, width - len(string))
    padding = ''.join(['0'] * n)
    return padding + string


# Check email syntax for validity
# This is a simplified version.  For comprehensive, use cleanEmail
def isValidEmail(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if re.fullmatch(regex, email):
        return True
    else:
        return False


# Comprehensive email validation and (attempted) cleaning
# For a list of the syntactical rules of valid email addresses,
# see: https://en.wikipedia.org/wiki/Email_address
# In particular this shows that many of the "email validation"
# regex you will find online is only approximately correct.
# If the email is invalid and can't be cleaned, the function returns
# an empty string
def cleanEmail(email):
    if not isinstance(email, str):
        print('cleanEmail passed ' + str(type(email)))
        return ''
    if email == '':
        return email
    email = email.strip().lower()
    split = email.split('@')

    # Remove emails with no "@"
    if len(split) < 2:
        print(' 1. Removing email with no "@"')
        return ''
    local_part = '@'.join(split[:-1])
    domain = split[-1]

    loc_srch = re.search('<(.*)', local_part)
    dom_srch = re.search('(.*)>', domain)
    if loc_srch and dom_srch:
        # Grab just email when in format "John Doe <john.doe@sample.com>
        # This is made more complex by the fact that "<@>"@sample.com is legal (hence last @ takes priority)
        local_part = loc_srch.group(1)
        domain = dom_srch.group(1)
    if local_part == '':
        # Bad: Empty local part
        print(' 2. Removing email with no local part')
        return ''
    if domain == '':
        # Bad: Empty domain
        print(' 3. Removing email with no domain')
        return ''

    # Remove (legal) comments in parens before or after local part
    pre_comment = re.search(r'^\(.*\)', local_part)
    if pre_comment:
        print(' 4. Removing start comment from email')
        local_part = local_part[pre_comment.end():]
    post_comment = re.search(r'\(.*\)$', local_part)
    if post_comment:
        print(' 5. Removing end comment from email')
        local_part = local_part[:post_comment.start()]

    # Remove addresses too long to be an email
    if len(local_part.encode('utf-8')) > 64 or len(domain.encode('utf-8')) > 255:
        print(' 6. Removing email that is too long')
        return ''

    last_ltr = ''
    in_quote = False
    escaped = False
    for ltr in local_part:
        if ltr == '"':
            if in_quote:
                if not escaped:
                    in_quote = False
                else:
                    escaped = not escaped
            elif last_ltr in ('', '.'):
                in_quote = True
            else:
                # Bad: Quoted strings must be dot-separated or the entire local part
                print(' 7. Removing email with startquote not dot-separated')
                return ''
        elif last_ltr == '"' and not in_quote and ltr != '.':
            # Bad: Endquotes must be dot-separated or at the end of the local part
            print(' 8. Removing email with endquote not dot-separated')
            return ''
        elif in_quote:
            if ltr == '\\':
                escaped = not escaped
            elif escaped:
                # Bad: Only backslashes and double quotes get escaped (otherwise this is an un-escaped backslash)
                print(' 9. Removing email with too many escape characters')
                return ''
        elif ltr == '.':
            if last_ltr in ('', '.'):
                # Bad: Dots can't be consecutive or the first character
                print('10. Removing email that breaks dot rules')
                return ''
        elif not ltr.isalnum() and ltr not in r'!#$%&\'*+-/=?^_`{|}~':
            # Bad: Other special characters not allowed in unquoted strings
            print('11. Removing email using illegal characters')
            return ''
        last_ltr = ltr
    if last_ltr == '.' or in_quote:
        # Bad: Dot can't be last character
        # Bad: Local part can't end with unterminated quote
        print('12. Removing email with bad last character or unterminated quote')
        return ''

    if len(domain) < 4:
        print('13. Removing email with too-short domain')
        return ''
    elif domain[0] == '[' and domain[-1] == ']':
        # Legal, but IP-style email domain not supported yet
        print('14. Removing email that uses IP-style domain')
        return ''
    # If missing a dot before top-level domain, add it
    domain = re.sub('(?<!\.)((?=com$)|(?=net$)|(?=org$)|(?=edu$)|(?=gov$))', '..', domain)
    spl = domain.split('.')
    if len(spl) < 2 or len(spl[-1]) < 2:
        # Technically legal, but dotless emails discouraged (and unsupported here)
        print('15. Removing email with dotless domain, or too-short top-level domain')
        return ''
    elif spl[-1] not in topLevelDomains:
        # Bad top-level domain
        print('16. Removing email with unlisted top-level domain')
        return ''
    domain = domain.lower()  # standardize to lowercase
    domain = re.sub('[;:,]', '..', domain)  # change any [;:,] to . in domain
    domain = re.sub('\.om$', '.com', domain)  # replace .om with .com (if at the end)

    domain = re.sub('[^a-zA-Z0-9.-]', '', domain)  # remove non-alphanumeric or . or -
    return local_part + '@' + domain


# cleans messy zipcode data
def cleanZip(zipcode, country='USA'):
    zipcode = str(zipcode)
    if country == 'USA':
        zipcode = re.sub('[^0-9-]', '', zipcode)
        if re.match('^[0-9]{1,5}-[0-9]{4}$', zipcode):
            zipcode = zipcode.split('-')[0]
        # assume too-short zip+4 is from leading zeros removed
        elif re.match('^[0-9]{6,9}', zipcode):
            zipcode = zipcode[:-4]
        elif re.match('[0-9]{10,}', zipcode):
            zipcode = zipcode[:5]
        zipcode = addLeadingZeros(zipcode, width=5)
        return zipcode
    elif country == 'CAN':
        zipcode = zipcode.upper()
        zipcode = re.sub('[^A-Z0-9]', '', zipcode)
        if not re.match('[A-Z][0-9][A-Z][0-9][A-Z][0-9]', zipcode):
            return ''
        return zipcode[:3] + ' ' + zipcode[3:6]
    # other country formats not implemented
    else:
        return zipcode.upper()


# validates zip / state combination
def cheapValidateUSZip(zipcode, state_abbr):
    # returns true for non-US states
    # intention is to only discard obviously bad data
    # run clean_zip() before using this function
    if not isinstance(state_abbr, str) or not isinstance(zipcode, str):
        return False

    state_abbr = state_abbr.upper()
    rgx = zipValidation.get(state_abbr, '')
    return re.match(rgx, zipcode)


# returns just the domain part of a url
# but discards business URLs hosted on large platforms like facebook
# since the domain will then not connect back to the business
def getDomain(url):
    # brief testing shows only the first three below are of note, rest probably not necessary but added for completeness
    host_domains = ['facebook.com', 'twitter.com', 'yext.com', 'bbb.com', 'youtube.com',
                    'instagram.com', 'yelp.com', 'angi.com', 'angieslist.com', 'ebay.com',
                    'linkedin.com', 'yellowpages.com', 'ethicalaz.com', 'ethicalcommunity.org',
                    'ethicalwesternpa.com', 'wisebuyingmall.com', 'trustab.org', 'gotrust.org',
                    'wordpress.com', 'bluehost.com']
    url = url.lower()  # standardize to lowercase
    url = re.sub('[;,]|(:(?!//))', '..', url)  # change any [;:,] to . in URL
    extracted = tldextract.extract(url)  # extract domain name
    domain = '.'.join(extracted[1:])  # and join suffix
    domain = re.sub('\.om$', '.com', domain)  # replace .om with .com (if at the end)
    if re.search('(^www)|(http)|(\.$)', domain):  # discard if any 'www' or 'http' still appearing,
        return ''  # or if domain + suffix ends in a period
    elif domain in host_domains:  # discard domains of hosting services
        return ''
    elif domain.split('.')[-1] not in topLevelDomains:
        return ''
    else:
        return domain


# Simple standardization for Business names
# to allow for more accurate comparisons
def standardizeName(name):
    name = name.lower()  # lowercase
    name = re.sub('&', ' and ', name)  # '&' --> 'and'
    name = re.sub('[^a-z\s]', '', name)  # remove punctuation
    name = re.sub(' {2,}', ' ', name)  # close multiple spaces caused by previous two steps
    return name


# Not needed for Yext / Uberall which is highly clean data
def validateZip(zipcode, stateprovince, country='US'):
    # intention is to only discard obviously bad data
    # run clean_zip() before using this function
    if not isinstance(stateprovince, str) or not isinstance(stateprovince, str):
        return False
    if len(stateprovince) != 2:
        stateprovince = stateProvinceToAbbrev.get(stateprovince.lower(), '')

    stateprovince = stateprovince.upper()
    rgx = zipValidation.get(stateprovince, '')
    return re.match(rgx, zipcode)
