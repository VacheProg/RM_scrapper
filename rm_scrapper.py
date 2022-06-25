import os
import pickle
import time
from concurrent import futures
import pandas as pd
import re
import requests
import json
from bs4 import BeautifulSoup
import pathlib
import urllib.parse as urlparse
import datetime
from . import utilities
from .Property import Property
import os
import time
from .utils import fork

def send_req(url, thread_id):
    try:
        r = requests.get(url, timeout=10, headers={'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36'})
        if r.status_code == 200:
            return r
        else:
            return None
    except Exception as why:
        print(why)
        return None

def get_pcode_id():
    """Read files where post code mapped with its id and returns dictionary key-post code, value- id"""
    post_codes = pd.read_csv(os.path.dirname(__file__) + '/post.csv', header=None, low_memory=False, sep=';')
    post_codes.columns = ['id', 'pcode']
    return dict(zip(list(post_codes['pcode']), list(post_codes['id'])))


def get_keychain(keychain, model):
    ret_val = []
    try:
        exec(f'ret_val.append({keychain})', {'ret_val':ret_val, 'model':model}, globals())
        return ret_val[0]
    except Exception as why:
        print(why, keychain)
        return None


class RMscraper:
    par = {'searchType': "", 'locationIdentifier': "", "insId": '1', "minPrice": "",
                   'maxPrice': '', 'minBedrooms': '', 'maxBedrooms': '', 'displayPropertyType': '',
                   'maxDaysSinceAdded': '1', 'sortByPriceDescending': '', '_includeLetAgreed': 'on',
                   'includeLetAgreed': 'true', 'primaryDisplayPropertyType': '', 'secondaryDisplayPropertyType': '',
                   'oldPrimaryDisplayPropertyType': '', 'letType': '', 'letFurnishType': '', 'houseFlatShare': '',
                   'oldDisplayPropertyType': '', 'includeSSTC': True
                   }
    data = get_pcode_id()
    def __init__(self, post_codes=None, full_posts=None, params=None):
        if params:
            self.params = params
        else:
            self.params = RMscraper.par
        self.sale_url = 'https://www.rightmove.co.uk/api/_search'
        self.post_codes = post_codes if post_codes else RMscraper.data.keys()
        self.full_post_codes = full_posts if full_posts is not None else []
        self.pcode2id = RMscraper.data
        self.main_data = pd.DataFrame()
        self.main_url = 'https://www.rightmove.co.uk'  
        self.all_data = []
        self.runner = fork.Fork()
        self.img_path = pathlib.Path('Images')
        self.sh_listing_id = re.compile('\w-(\d+)-(\d+)')
        self.__listing_reg = re.compile('(\d+)')
        self.sh_json_kew_vals = {'proprty_id':"model['soldPropertyData']['property']['id']", 
                                'address':"model['soldPropertyData']['displayAddress']", 
                                'type':"model['soldPropertyData']['propertyType']", 
                                'subtype':"model['soldPropertyData']['property']['propertySubType']", 
                                'bedrooms':"model['soldPropertyData']['property']['bedrooms']", 
                                'bathrooms':"model['soldPropertyData']['property']['bathrooms']", 
                                'features':"model['soldPropertyData']['property']['keyFeatures']", 
                                'longitude':"model['soldPropertyData']['property']['location']['longitude']", 
                                'latitude':"model['soldPropertyData']['property']['location']['latitude']", 
                                'delivery_point_id':"model['soldPropertyData']['property']['address']['deliveryPointId']", 
                                'outcode': "model['soldPropertyData']['property']['address']['outcode']",
                                'incode': "model['soldPropertyData']['property']['address']['incode']",
                                'country':"model['soldPropertyData']['property']['address']['ukCountry']", 
                                'pinType': "model['soldPropertyData']['property']['location']['pinType']",
                                'transactions': "model['soldPropertyData']['transactions']",
                                'imgs': "model['soldPropertyData']['property']['images']",
                                '_added':"model['soldPropertyData']['property']['listingHistory']['listingUpdateReason']",
                                'transanction_id': "model['metadata']['transactionId']"}
        self.sh_initial_key_vals = {'scrape_date':datetime.datetime.now(),
                                    'list_type':'sold_history',
                                    'website': 'https://www.rightmove.co.uk/'}
        self.sh_overall = { 'full_postcode': "' '.join([model['outcode'], model['incode']])", 
                            'is_correct': "model['pinType'] == 'ACCURATE_POINT'",
                            'images': "[i['url'] for i in model['imgs']]",
                            'status': "model['_added'].split()[0]",
                            'Update_date': "datetime.datetime.strptime(model['_added'].split()[-1], '%d/%m/%Y')",
                            'Path': "'Images/'+model['transanction_id']+'_'+model['proprty_id']"}
        self.listing_json = {'proprty_id':"model['propertyData']['id']", 
                            'price_prime':"model['propertyData']['prices']['primaryPrice']",
                            'price_second':"model['propertyData']['prices']['secondaryPrice']",
                            'currency':"model['analyticsInfo']['analyticsProperty']['currency']", 
                            'address':"model['propertyData']['address']['displayAddress']", 
                            'type':"model['propertyData']['soldPropertyType']", 
                            'subtype':"model['propertyData']['propertySubType']", 
                            'bedrooms':"model['propertyData']['bedrooms']", 
                            'bathrooms':"model['propertyData']['bathrooms']", 
                            'features':"model['propertyData']['keyFeatures']", 
                            'longitude':"model['propertyData']['location']['longitude']", 
                            'latitude':"model['propertyData']['location']['latitude']", 
                            'delivery_point_id':"model['propertyData']['address']['deliveryPointId']", 
                            'outcode': "model['propertyData']['address']['outcode']",
                            'incode': "model['propertyData']['address']['incode']",
                            'country':"model['propertyData']['address']['ukCountry']", 
                            'pinType': "model['propertyData']['location']['pinType']",
                            'date':"model['analyticsInfo']['analyticsProperty']['added']",
                            'tags': "model['propertyData']['tags']",
                            'tenure': "model['propertyData']['tenure']['tenureType']",
                            'description': "model['propertyData']['text']['description']", 
                            'virtual_tour_links': "model['propertyData']['virtualTours']",
                            'comercial': "model['propertyData']['commercial']",
                            'business_for_sale': "model['propertyData']['businessForSale']",
                            'ownership': "model['analyticsInfo']['analyticsProperty']['ownership']",
                            'preowned': "model['analyticsInfo']['analyticsProperty']['preOwned']",
                            #AGENT INFO
                            'barnch_id': "model['propertyData']['customer']['branchId']",
                            'branch_name': "model['propertyData']['customer']['branchName']",
                            'branch_display_name': "model['propertyData']['customer']['branchDisplayName']",
                            'company_name': "model['propertyData']['customer']['companyName']",
                            'company_trading_name': "model['propertyData']['customer']['companyTradingName']",
                            'customer_profile_url': "model['propertyData']['customer']['customerProfileUrl']",
                            'agent_type': "model['analyticsInfo']['analyticsBranch']['agentType']",
                            'company_type': "model['analyticsInfo']['analyticsBranch']['companyType']",
                            'phone_number': "model['propertyData']['contactInfo']['telephoneNumbers']['localNumber']",
                            }
        self.sale_json = dict(self.listing_json, **{'years_remaining_on_lease': "model['propertyData']['tenure']['yearsRemainingOnLease']"})
        self.rent_json = dict(self.listing_json, **{'fees_apply_text':"model['['propertyData']['feesApply']']",
                                                   'deposit': "",
                                                   'let_type': "",
                                                   'furnished': "",
                                                   'available_date': "",})

        self.listing_overall = { 'full_postcode': "' '.join([model['outcode'], model['incode']])", 
                                 'is_correct': "model['pinType'] == 'ACCURATE_POINT'",
                                 'date':"datetime.datetime.strptime(model['date'], '%Y%m%d')"}
                        # # initial_Data 
                        # ini ={'scrape_date':datetime.datetime.now(),
                        #       'list_type':'sold_history', 
                        #       'website': 'https://www.rightmove.co.uk/'
                        #     'list_type':"",

                        #      }

    def __update_params(self, params):
        """Aim of this function to update  self default params  with given one"""
        self.params.update(params)

    def _get_and_run_property_urls(self, resps):
        """a"""
        in_case = []
        for i, j in resps.items():
            try:
                for ind, url in enumerate([self.main_url + x['propertyUrl'] for x in j.json()['properties']]):
                    self.runner.add(target=send_req, **{'url':url, 'thread_id': f'{i}_{ind}'})
                    print(url)
                in_case.extend(j.json()['properties'])
            except:
                pass
        pd.DataFrame(in_case).to_csv('pcode_res.csv')

    def get_listing(self, params, down_imgs, buy_sell='BUY'):
        download_list = {}
        if params is not None:
            self.__update_params(params)
        pcode_urls = {}
        self.runner.run(concurrent_limit=40)
        for post in list(self.post_codes):
            pcode_urls[post] = []
            # PAGE COUNT
            for i in [0, 24]:
                self.params['locationIdentifier'] = 'OUTCODE^' + str(self.pcode2id[post])
                self.params['channel'] = buy_sell
                self.params['index'] = i
                self.runner.add(target=send_req, **{'url':self.__edit_url_by_params(self.sale_url, self.params), 'thread_id':f'{post}_i'})
                # pcode_urls[post].append(self.__edit_url_by_params(self.sale_url, self.params))
        print('STARTING')
        res = self.runner.wait_and_get_results()
        self.runner.results = {}
        self.runner.finish()
        self.runner.run(concurrent_limit=40)
        print('zzzzzzzz', res)
        # pcode_to_resp, errors = utilities.get_urls(pcode_urls, max_workers=40)
        self._get_and_run_property_urls(res)
        prop_resps = self.runner.wait_and_get_results()
        self.runner.finish()
        print("GETTING PROPERTIES")
        for key, data in prop_resps.items():
            self.__run_per_property(data, download_list)
        print('Downloading images this may take a while')
        self.main_data = self.main_data.append(self.all_data)
        name = 'sale_data.csv' if buy_sell =='BUY' else 'rent_data.csv'
        self.main_data.to_csv(name)
        if down_imgs:
            utilities.save_imgs(download_list, max_workers=40)

    def __run_per_property(self, resp, download_list):
        row = dict()
        page_model = json.loads(re.search(r'window.PAGE_MODEL = (.*)', resp.text, re.MULTILINE).group(1))
        current_house_path = self.img_path.joinpath(f'{page_model["propertyData"]["id"]}')
        if not os.path.isdir(current_house_path):
            os.makedirs(current_house_path)
        self.__parse_json(self.sh_initial_key_vals, self.listing_json, self.listing_overall, page_model, row)
        self.all_data.append(row)

    def get_sold(self, params=None, down_imgs=True):
        """Gets sold data from Rightmove"""
        self.get_listing(params, down_imgs, 'BUY')

    def get_rent(self, params=None, down_imgs=True):
        self.get_listing(params, down_imgs, 'RENT')

    def __save_res(self, i, img_downs):
        t1 = time.time()
        print('STARTING CHUNKE NO: ', i)
        df_list = []
        all_imgs = {}
        first_time = not os.path.isfile('sold_history.csv')
        prop_resps = self.runner.wait_and_get_results()
        for key, val in prop_resps.items():
            r1, r2 = self.__get_prop_data(val, key)
            df_list.append(r1)
            all_imgs.update(r2)
        print('DONE', key)
        print("DONE CHUNK NO:", i)
        print('SPENT TIME', time.time()-t1)
        self.runner.finish()
        self.runner.results = {}
        self.runner.run(concurrent_limit=30)
        df = pd.DataFrame(df_list)
        if first_time:
            df.to_csv('sold_history.csv')
            df.to_pickle('a.pkl')
        else:
            df.to_csv('sold_history.csv', mode='a', header=False)
            df.to_pickle('a.pkl')
        self.runner.results = {}
        self.runner.run(concurrent_limit=40)
        cnt = 0
        if img_downs:
            name = f'img_dict_{cnt}.pkl'
            if os.path.isfile(name):
                cnt+=1
            else:
                with open(name, 'wb') as f:
                    pickle.dump(all_imgs, f)
            print(all_imgs)
            utilities.save_imgs(all_imgs, max_workers=40)
        first_time=False


    def get_sold_history(self, pcodes=None, page_size=5, foreign_key=False, img_downs=True):
        """Getting sold history for specified postcodes in RIghtmove..."""
        search_url = 'https://www.rightmove.co.uk/house-prices/{pcode}.html?&soldIn=1&page={ind}'
        post_urls = {}
        self.runner.run(concurrent_limit=40)

        post_codes = self.post_codes if pcodes is None else pcodes
        for ind,  post in enumerate(post_codes):
            for page in range(1, page_size):
                key = post if foreign_key is None else ind
                self.runner.add(target=send_req, **{'url':search_url.format(pcode='-'.join(post.lower().split()), ind=page), 'thread_id':f'{ind}_{page}'})
        post_resps = self.runner.wait_and_get_results()
        self.runner.results = {}
        self.runner.finish()
        self.runner.run(concurrent_limit=40)

        print('DONE POSTCODE REQS')
        with open('reqs_done_pc.pkl', 'wb') as f:
            pickle.dump(post_resps, f)
        # with open('reqs_done_pc.pkl', 'rb') as f:
        #     post_resps = pickle.load(f)
        prop_urls = self.__get_prop_urls(post_resps, foreign_key)
        del post_resps
        print('PROP URLS ARE: ', len(prop_urls))
        for i, (k, v) in enumerate(prop_urls.items(), 1):
            if i<=330000:
                continue
            self.runner.add(target=send_req, **{'url': v, 'thread_id':k})
            if (i%100) == 0:
                self.__save_res(i, img_downs=img_downs)
                # time.sleep(10*60)
        else:
            self.__save_res(i, img_downs=img_downs)
            self.runner.finish()


    def find_full_address(self, listings_csv, sh_csv):
        # TODO to be implemented
        """Aim of this function to find full address for the scrapped data
            This function uses local class variables, make sure to run at first scrapping part then use this function"""

        sh = pd.read_csv(sh_csv)
        listings = pd.read_csv(listings_csv)




    def __get_prop_urls(self, post_resps, foreign_key):
        """AIM TO ITERATE OVER POST RESPONDS AND COLLECT PROP URLS"""
        prop_urls = {}
        for for_key, resp in post_resps.items():
            # for r in resp:
            if resp is None:
                continue
            page = resp.text
            cont = re.search(r'window.__PRELOADED_STATE__ = (.*}})</script>', page)
            if cont is not None:
                json_obj = json.loads(cont.group(1))
                for prop in (i for i in json_obj['results']['properties'] if i['detailUrl']):
                    listing_id = re.search(self.sh_listing_id, prop['detailUrl'])
                    if listing_id is None:
                        continue
                    key = (f'{self.img_path}/{listing_id.group(1)}_{listing_id.group(2)}', for_key.split('_')[0]) if foreign_key\
                        else f'{self.img_path}/{listing_id.group(1)}_{listing_id.group(2)}'
                    prop_urls[key] = prop['detailUrl']
        return prop_urls

    # def __filter_prop(self, js_obj, filters):
    #     if all(self.filt_to_func[flt](js_obj, filters[flt]) for flt in filters):
    #         return True
    #     return False

    def __parse_json(self, ini, from_js, post_eval, model, data):
        data.update(ini)
        for k, val in from_js.items():
            data[k] = get_keychain(val, model)
        for k, val in post_eval.items():
            data[k] = get_keychain(val, data)
        # return data

    def __get_prop_data(self, prop, id_):
        """Retrieves data from property"""
        d = {}
        data = {}
        if type(id_) == tuple:
            data['foreign_key'] = id_[1]
            #id_
        if prop is None:
            return data, d
        page = prop.text
        model = json.loads(re.search(r'window.PAGE_MODEL = (.*)', page, re.MULTILINE).group(1))
        self.__parse_json(self.sh_initial_key_vals, self.sh_json_kew_vals, self.sh_overall, model, data)
        for ind, i in enumerate(data['images']):
            d[i] = f"{data['Path']}/image_{ind}.{i.split('.')[-1]}"
        return data, d

    def __parse_one_resp(self, resp, urls, id_, down_list, foreign, data, thread_id):
        if (not resp) or (resp.status_code != 200):
            return
        row = {'Property URL': urls[id_]}
        model = json.loads(re.search(r'window.PAGE_MODEL = (.*)', resp.text, re.MULTILINE).group(1))
        path = pathlib.Path(f'Images/').joinpath(re.search(self.__listing_reg, urls[id_]).group(0))
        if not os.path.isdir(path):
            os.makedirs(path)
        prop = Property(path, row, model, down_list)
        if foreign:
            row['foreign_key'] = id_
        prop.run()
        data.append(row)


    def get_properties_by_url(self, urls, to_csv=True, foreign_key=False, down_imgs=True):
        """Gets list of urls and creates csv file from their data"""
        self.runner.run(concurrent_limit=40)
        for i, u in enumerate(urls):
            self.runner.add(target=send_req, **{'url': u, 'thread_id': i})
        print('Done Getting URLS')
        data = []
        down_list = {}
        _results = self.runner.wait_and_get_results()
        print('GOT URLS')
        for i in range(len(urls)):
            resp = _results[i]
            self.runner.add(target=self.__parse_one_resp, **{'resp': resp, 'urls': urls, 'id_':i, 'down_list': down_list, 'foreign':foreign_key, 'data':data, 'thread_id':i})
        _results = self.runner.wait_and_get_results()
        print('DONE respparse')
        self.runner.finish()
        if down_imgs:
            utilities.save_imgs(down_list, max_workers=40)
        print('len(data): ', len(data))
        if to_csv:
            db = pd.DataFrame(data)
            db.to_csv('by_url.csv')
        else:
            return data

    def __edit_url_by_params(self, url, params):
        url_parse = urlparse.urlparse(url)
        url_new_query = urlparse.urlencode(params)
        url_parse = url_parse._replace(query=url_new_query)
        new_url = urlparse.urlunparse(url_parse)
        return new_url
