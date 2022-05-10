# -*- coding: utf-8 -*-
import requests
import pandas as pd

# function to call Stats NZ open data api

def get_odata(service, endpoint, entity, query_option, api_key, proxies):
    
# setup variables    
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    proxies = proxies
    url = service + '/' + endpoint + '/' + entity + '?' + query_option
    top_query = "$top" in query_option
    results = pd.DataFrame()

    # continue getting results while there are more pages 
    while url:
    
        try:
            r = requests.get(url,headers=headers,proxies=proxies,verify=False) # Stop SSL verification as requests module bugged
            r.raise_for_status()
    
        # raise request errors        
        except requests.HTTPError as exception:
            print(exception)
            print(r.text)
            break
            
        df = pd.json_normalize(r.json()['value'])
        results = pd.concat([results,df])
        
        # get the next page url
        try:
            url = r.json()['@odata.nextLink'] 
            # return just the first page if $top was used
            if top_query:
                url = None
        except KeyError:
            url = None
        # show progress    
        print('.', end = ' ', flush = True)
    # Convert Period column to datetime
    try:
        results['Period'] = pd.to_datetime(results['Period'], format='%Y/%m/%d')
    except ValueError:
        print('Non-date values in dataframe column')
    except KeyError:
        pass
    
    print(len(results.index),'Obs retrieved')

    return results

