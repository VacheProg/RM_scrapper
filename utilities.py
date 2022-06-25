"""Aim of this module to create synchronized and  parallel utilities for all the project"""
import os

import requests
import shutil
from concurrent import futures

# img_urls = ['https://media.istockphoto.com/photos/yerevan-capital-of-armenia-in-front-of-mt-ararat-picture-id1144776438?k=20&m=1144776438&s=612x612&w=0&h=Qg8l3Ibz1fAJlS8NaV6sNHLocTZweUh1il9rGucgdX4=',
#             'https://image.shutterstock.com/image-photo/view-mountain-ararat-yerevan-city-260nw-1450464164.jpg',
#             'https://media.gettyimages.com/photos/mount-ararat-view-from-the-cascade-complex-yerevan-armenia-picture-id1157377939?s=612x612',
#             'https://media.gettyimages.com/photos/armenia-yerevan-republic-square-dancing-fountains-picture-id1068746262?s=612x612',
#             'https://media.istockphoto.com/photos/cascade-yerevan-picture-id500221043?k=20&m=500221043&s=612x612&w=0&h=631zq-LelZRcSWdOQUK3gX0ZK3gTnGaMkR_YAh8kLmM=']

def save_imgs(img_urls_paths: dict, proxy=None, max_workers=10):
    """Function gets image url to directory path map and parallel downloads it
        Image can be url or binary
        In the path name should be included file extension as well if not, function will try to parse from url
        Proxy is the list of proxies, if given code will run parallel from given proxies"""
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(save_img, img_data, path):path for (img_data, path) in img_urls_paths.items()}
        for future in futures.as_completed(future_to_url):

            path = future_to_url[future]
            # print("DONE ", path)
    # for img_data, file_name in img_urls_paths.items():
    #     save_img(img_data, file_name)


def send_request(urls, _id, headers=None, proxy=None):
    result = []
    errors = []
    if type(urls) == list:
        for url in urls:
            try:
                k = requests.get(url, headers=headers, proxies=proxy)
                result.append(k)
                # res = check_resp(resp)
            except Exception as why:
                errors.append(f"Exception was raised while getting URL:{url}\n Error:{why}")
    else:
        try:
            result.append(requests.get(urls, headers=headers, proxies=proxy))
        except Exception as why:
            errors.append(f"Exception was raised while getting URL:{urls}\n Error:{why}")
    return result, errors


def save_img(img_data, file_name):
    if type(img_data) == str:
        resp = requests.get(img_data, stream=True)
        resp.raw.decode_content = True
        buffer = resp.raw
    else:
        buffer = img_data
    dr_name = os.path.dirname(file_name)
    if dr_name and not os.path.isdir(dr_name):
        os.makedirs(dr_name)
    with open(file_name, 'wb') as f:
        shutil.copyfileobj(buffer, f)

def get_urls(url_list, headers=None, proxies=None, paralel=True, max_workers=10):
    # TODO FINALIZE THIS PLS TO HAVE SPECIFIC API
    errors = {}
    result = {}
    if type(url_list) == list:
        iterable = enumerate(url_list)
    elif type(url_list) == dict:
        iterable = url_list.items()
    else:
        iterable = url_list
    if paralel:
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(send_request, url, _id, headers, proxies): (_id, url) for _id, url in iterable}
            for future in futures.as_completed(future_to_url):
                _id, url = future_to_url[future]
                errors[_id] = []
                try:
                    data, err = future.result()
                    result.setdefault(_id, []).extend(data)
                    errors.setdefault(_id, []).extend(err)
                except Exception as exc:
                    errors[_id].append(f'Exception raised in Concurrent for URL: {url}\n URL index:{_id}\n Error: {exc}', _id)
    return result, errors