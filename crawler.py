#! /usr/bin/env python3

import os
import time
import math
import urllib.parse
import requests
import shutil
import argparse
import tempfile
from typing import Optional, Iterator
import random
import re
import threading
from bs4 import BeautifulSoup
import psutil
import signal

HERE = os.path.dirname(__file__)

FOLDER_ROOT = os.path.join(HERE, 'root')

FOLDER_DEDUP    = os.path.join(FOLDER_ROOT, 'dedup')
FOLDER_DOWNLOAD = os.path.join(FOLDER_ROOT, 'download')
FOLDER_SCAN     = os.path.join(FOLDER_ROOT, 'scan')
FOLDER_SAVE     = os.path.join(FOLDER_ROOT, 'save')
FOLDER_DONE     = os.path.join(FOLDER_ROOT, 'done')

FOLDER_DEDUP_FAIL    = os.path.join(FOLDER_ROOT, 'dedup_fail')
FOLDER_DOWNLOAD_FAIL = os.path.join(FOLDER_ROOT, 'download_fail')
FOLDER_SCAN_FAIL     = os.path.join(FOLDER_ROOT, 'scan_fail')
FOLDER_SAVE_FAIL     = os.path.join(FOLDER_ROOT, 'save_fail')

FOLDER_DOMAIN_INFO = os.path.join(FOLDER_ROOT, 'domain_info')

FILE_ERROR_LOG = os.path.join(FOLDER_ROOT, 'error_log')

FILENAME_LINK = 'link'
FILENAME_DATA = 'data'

DOMAIN_COOLDOWN = 5 # how much time to wait between newtwork requests for each domain
DOMAIN_MTIME_CHECK_SAFETY_SLEEP_MAX = 0.1 # random amount of time to wait between 0 and this, so that it's much less likely to collide with another thread checking for the mtime of the same domain

THR_NODE_ID_MULTIPLIER = 100 # making this higher makes thread offloading more random; making this too high will fuck shit up and make only 1 thread do all
                             # the work; see the output of time.time()
THR_LOOP_DONE_SLEEP = 5.0

# functions logs

def log_error(text:str) -> None:
    text = text + '\n'
    with open(FILE_ERROR_LOG, 'a') as f:
        f.write(text)

# functions data processing

def extract_links_from_file(path:str, website:str) -> list[str]:
    # TODO? find a better way

    if website.endswith('/'):
        website = website[:-1]

    urls = []

    with open(path) as f:

        try:
            data = f.read()
        except UnicodeDecodeError:
            return []


        # https://uibakery.io/regex-library/url-regex-python
        url_extract_pattern = "https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)"
        urls.extend(re.findall(url_extract_pattern, data))


        # # https://stackoverflow.com/questions/499345/regular-expression-to-extract-url-from-an-html-link
        # found = re.findall(r'href=[\'"]?([^\'" >]+)', data)

        # for idx, link in reversed(list(enumerate(found))):

        #     if link.startswith('#'):
        #         del found[idx]

        #     elif link.startswith('/'):
        #         found[idx] = f'{website}{link}'
            
        #     else:
        #         domain = extract_link_domain(link)
        #         if len(domain) == 0:
        #             found[idx] = f'{website}/{link}'

        # urls.extend(found)


        # https://www.geeksforgeeks.org/extract-all-the-urls-from-the-webpage-using-python/

        soup = BeautifulSoup(data, 'html.parser')
        # TODO make it shut up if I'm parsing an XML
        # TODO automatically determine content type

        for link in soup.find_all('a'):
            link = link.get('href')
            if link is None:
                continue
            urls.append(link)


    urls_clean = []

    for url in urls:

        if url.startswith('#'):
            continue
        
        if url.startswith('/'):
            url = website + url

        if url not in urls_clean:
            urls_clean.append(url)

    return urls_clean

# functions networking

def extract_link_domain(link:str) -> str:
    url = urllib.parse.urlparse(link)
    
    domain = url.netloc # sex.stackoverflow.com:6969

    if ':' in domain:
        idx = domain.index(':')
        domain = domain[:idx] # sex.stackoverflow.com

    domain = domain.split('.') # type: ignore # ['sex', 'stackoverflow', 'com']
    if len(domain) > 2:
        domain = domain[-2:] # ['stackoverflow', 'com']

    domain = '.'.join(domain)

    return domain

def extract_link_website(link:str) -> str:
    data = urllib.parse.urlparse(link)
    return f'{data.scheme}://{data.netloc}'

def download_to_file(file:str, link:str) -> Optional[bool]:
    # TODO make it so that the download happens gradually

    # TODO this is not the best place for this
    from urllib3.exceptions import InsecureRequestWarning # type: ignore
    import requests.packages.urllib3 # type: ignore
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    try:
        response = requests.get(link, verify=False) # do not check certificates
    except requests.exceptions.ConnectionError:
        log_error(f'could not open link `{link=}`')
        return False

    if response.status_code == 429: # too many requests
        return None

    if not response.ok:
        return False
    
    with open(file, 'wb') as f: # TODO dano raboti, ne sum testval
        f.write(response.content)
    
    return True

# functions HDD

def gen_filename() -> str:
    return f'{time.time()}'

def read_file_line(file:str) -> str:
    with open(file) as f:
        #return f.read()
        data = f.readline()
        if data.endswith('\n'):
            data = data[:-1]
        return data

def write_file(file:str, data:str) -> None:
    with open(file, 'w') as f:
        f.write(data)

def get_mtime(file:str) -> float:
    return os.path.getmtime(file)

def update_mtime(file:str) -> None:
    os.utime(file)

def gen_file() -> str:
    with tempfile.NamedTemporaryFile(dir='/var/tmp', delete=False) as f:
        return f.name

def gen_directory() -> str:
    return tempfile.mkdtemp(dir='/var/tmp')

def move_node(path_old:str, path_new:str) -> None:
    shutil.move(path_old, path_new)

def delete_file(path:str) -> None:
    assert os.path.isfile(path)
    os.remove(path)

def delete_folder(path:str) -> None:
    assert os.path.isdir(path)
    shutil.rmtree(path)

def string_as_nested_folders(string:str) -> str:
    path = ''

    for char in string:
        code = str(ord(char))
        path = os.path.join(path, code)
    
    return path

# functions thread code

def get_nodes_that_are_to_be_processed_by_this_thread(folder:str, folder_fail:str, thread_id:int, number_threads:int, *, use_files:bool) -> Iterator[tuple[str, str]]:
    nodes = None
    for path, folders, files in os.walk(folder):
        nodes = files if use_files else folders
        break
    if nodes is None: # folder is empty
        return
    
    for node_name in nodes:
        node_path = os.path.join(path, node_name)

        try:
            node_id = float(node_name)
        except ValueError:
            print(f'ERROR: invalid node name `{node_name}`; moving to failed folder `{folder_fail}`')
            move_node(node_path, os.path.join(folder_fail, node_name))
            continue

        if math.floor((node_id * THR_NODE_ID_MULTIPLIER) % number_threads) != thread_id:
            # some other thread's file
            continue
        
        yield node_name, node_path

def link_has_already_been_processed_not_too_long_ago(link:str) -> bool:
    # TODO? scan all folders and not just done

    as_nested_folder_path = string_as_nested_folders(link)

    folder_done = os.path.join(FOLDER_DONE, as_nested_folder_path)

    if not os.path.isdir(folder_done):
        return False
    
    for _path, _folders, files in os.walk(folder_done):
        break
    if len(files) > 0:
        # TODO check the mtime (or the file name)
        return True
    
    return False

# threads

def thr_dedup(thread_id:int, number_threads:int) -> None:
    while True:

        for file_name, file_path in get_nodes_that_are_to_be_processed_by_this_thread(FOLDER_DEDUP, FOLDER_DEDUP_FAIL, thread_id, number_threads, use_files=True):

            link = read_file_line(file_path)
            # if '\n' in link:
            #     idx = link.index('\n')
            #     link = link[:idx]

            # TODO? decode URL (what is url is decoded but has weird characters in name that makes decoding it again fuck it up?)

            # print(f'processing link: {link}')

            # scan

            if link_has_already_been_processed_not_too_long_ago(link):
                delete_file(file_path)
            else:
                move_node(file_path, os.path.join(FOLDER_DOWNLOAD, file_name))

        time.sleep(THR_LOOP_DONE_SLEEP)

def thr_download(thread_id:int, number_threads:int) -> None:
    while True:
        
        for file_name, file_path in get_nodes_that_are_to_be_processed_by_this_thread(FOLDER_DOWNLOAD, FOLDER_DOWNLOAD_FAIL, thread_id, number_threads, use_files=True):

            link = read_file_line(file_path)

            domain = extract_link_domain(link)
            if len(domain) == 0:
                log_error(f'len(domain) == 0; `{link=}`')
                delete_file(file_path)
                continue

            # print(f'processing domain `{domain}`; {file_path=}')

            # check mtime

            domain_mtime_file = os.path.join(FOLDER_DOMAIN_INFO, domain) # assuming that it's possible to create filenames for each domain in existance

            time.sleep(random.uniform(0, DOMAIN_MTIME_CHECK_SAFETY_SLEEP_MAX))

            if os.path.isfile(domain_mtime_file):
                mtime = get_mtime(domain_mtime_file)
                now = time.time()

                if abs(now - mtime) < DOMAIN_COOLDOWN: # using abs in case the metadata is bad
                    continue

                update_mtime(domain_mtime_file)
            else:
                write_file(domain_mtime_file, '')

            # download data

            # print(f'downloading data from link `{link}`; {file_path=}')

            file_data_tmp = gen_file()
            succ = download_to_file(file_data_tmp, link)
            if succ is True:
                # print(f'successfully downloaded data from link: {link}')
                pass
            else:
                delete_file(file_data_tmp)

                if succ is False:
                    # TODO save this to another file to try again later?
                    delete_file(file_path)
                    # print(f'could not download data from link: {link}')
                    continue
                elif succ is None: # too many requests
                    print(f'link rate limited: {link}')
                    continue # TODO somehow indicate that this domain is overloaded
                else:
                    assert False

            folder_root = gen_directory()
            file_link = os.path.join(folder_root, FILENAME_LINK)
            file_data = os.path.join(folder_root, FILENAME_DATA)

            write_file(file_link, link)
            move_node(file_data_tmp, file_data)

            # move the whole thing

            move_node(folder_root, os.path.join(FOLDER_SCAN, file_name))
            delete_file(file_path)

        time.sleep(THR_LOOP_DONE_SLEEP)

def thr_scan(thread_id:int, number_threads:int) -> None:
    while True:

        for folder_name, folder_path in get_nodes_that_are_to_be_processed_by_this_thread(FOLDER_SCAN, FOLDER_SCAN_FAIL, thread_id, number_threads, use_files=False):

            folder_root = folder_path
            file_link = os.path.join(folder_root, FILENAME_LINK)
            file_data = os.path.join(folder_root, FILENAME_DATA)

            link = read_file_line(file_link)
            website = extract_link_website(link)

            # print(f'scanning for links in: {file_data}')

            for link in extract_links_from_file(file_data, website):
                name = gen_filename()
                write_file(os.path.join(FOLDER_DEDUP, name), link)

            # move

            move_node(folder_root, os.path.join(FOLDER_SAVE, folder_name))

        time.sleep(THR_LOOP_DONE_SLEEP)

def thr_save(thread_id:int, number_threads:int) -> None:
    while True:

        for folder_name, folder_path in get_nodes_that_are_to_be_processed_by_this_thread(FOLDER_SAVE, FOLDER_SAVE_FAIL, thread_id, number_threads, use_files=False):

            folder_root = folder_path
            file_link = os.path.join(folder_root, FILENAME_LINK)
            file_data = os.path.join(folder_path, FILENAME_DATA)

            # print(f'saving folder: {folder_root}')
            
            if os.path.isfile(file_link) and os.path.isfile(file_data):
                link = read_file_line(file_link)
                nested_folders_name = string_as_nested_folders(link)

                location = os.path.join(FOLDER_DONE, nested_folders_name)
                os.makedirs(location, exist_ok=True)

                location = os.path.join(location, gen_filename()) # this does mean that we'll keep the old versions (this might get heavy)
                move_node(file_data, location)
            else:
                print(f'ERROR: corrupted folder `{folder_path}`')
            
            delete_folder(folder_path)
        
        time.sleep(THR_LOOP_DONE_SLEEP)

# thread starter

def main(dedup_threads:int, download_threads:int, scan_threads:int, save_threads:int) -> None:

    # makesure folders exist

    folders = (
        FOLDER_DEDUP,
        FOLDER_DOWNLOAD,
        FOLDER_SCAN,
        FOLDER_SAVE,
        FOLDER_DONE,

        FOLDER_DEDUP_FAIL,
        FOLDER_DOWNLOAD_FAIL,
        FOLDER_SCAN_FAIL,
        FOLDER_SAVE_FAIL,

        FOLDER_DOMAIN_INFO,
    )

    for folder in folders:
        os.makedirs(folder, exist_ok=True)

    # run the daemons

    pairs_fnc_threads = (
        (thr_dedup, dedup_threads),
        (thr_download, download_threads),
        (thr_scan, scan_threads),
        (thr_save, save_threads),
    )

    pids = []

    for fnc, num_threads in pairs_fnc_threads:
        for thr_id in range(num_threads):

            pid = os.fork() # TODO use multiprocessing so that if the parent dies the children also die (kinda)
            if not pid: # child
                fnc(thr_id, num_threads)
                return
            
            pids.append(pid)

            # threading.Thread(target=fnc, args=(thr_id, num_threads)).start()

    # kill_all = False

    # while not kill_all:
    #     time.sleep(4)
    #     for pid in pids:
    #         if not psutil.pid_exists(pid):
    #             print(f'ERROR: daemon has died; {pid=}')
    #             kill_all = True
    #             break
    
    # for pid in pids:
    #     os.kill(pid, signal.SIGTERM)

    # time.sleep(4)

    # for pid in pids:
    #     os.kill(pid, signal.SIGKILL)

    while True:
        time.sleep(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dedup_daemons', type=int)
    parser.add_argument('download_daemons', type=int)
    parser.add_argument('scan_daemons', type=int)
    parser.add_argument('save_daemons', type=int)
    args = parser.parse_args()

    main(args.dedup_daemons, args.download_daemons, args.scan_daemons, args.save_daemons)