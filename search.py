#! /usr/bin/env python3

import argparse
import os
from typing import Optional
import subprocess

import termcolor

from crawler import FOLDER_DONE
from crawler import nested_folders_as_string

RESULT_COLOR = 'red'
FILE_PATH_COLOR = 'dark_grey'

# returns files that contain the given text
def grep(text:str, node:str) -> list[str]:
    shell = subprocess.run(['grep', '-rIil', '--', text, node], capture_output=True)
    if shell.returncode != 0:
        return []

    files = shell.stdout.decode().split('\n')
    while '' in files:
        files.remove('')

    return files

# def links_that_contain_search_term(search_term:str) -> list[str]:
#     # out = term(['grep', '-rIil', '--', search_term, FOLDER_DONE])

#     # out = out.split('\n')
#     # while '' in out:
#     #     out.remove('')

#     out = grep(search_term, FOLDER_DONE)

#     for idx, item in enumerate(out):
#         item = item[len(FOLDER_DONE):]
#         if item.startswith('/'):
#             item = item[1:]
#         out[idx] = item

#     return out

# def file_contains_search_term(file:str, search_term:str) -> bool:
#     try:
#         term(['grep', '-rIil', '--', search_term, file]) # TODO duplicate of the grep above
#     except subprocess.CalledProcessError:
#         return False
#     return True

# def grep(search_term:str, search_in:Optional[list[str]]=None) -> list[str]:
#     if search_in is None:
#         return links_that_contain_search_term(search_term)
    
#     valid = []
#     for file in search_in:
#         if file_contains_search_term(file, search_term):
#             valid.append(file)
    
#     return valid

def main() -> None:

    initial_search = True
    files_containing_search_terms:list[str] = []

    while True:
        print()
        search_term = input('Enter search term: ')

        if initial_search:
            initial_search = False
            files_containing_search_terms = grep(search_term, FOLDER_DONE)
        else:
            for idx, file in reversed(list(enumerate(files_containing_search_terms))):
                if len(grep(search_term, file)) == 0:
                    del files_containing_search_terms[idx]
        
        print()
        for file in files_containing_search_terms:

            name = file

            name = name[len(FOLDER_DONE):]

            if name.startswith('/'):
                name = name[1:]

            name = os.path.dirname(name) # get rid of the folder

            name = nested_folders_as_string(name)

            print()
            print(termcolor.colored(name, RESULT_COLOR))
            print(termcolor.colored(file, FILE_PATH_COLOR))

    # is_first_term = True

    # while True:
    #     print()
    #     search_term = input('Enter search term: ')

    #     if is_first_term:
    #         is_first_term = False
    #         results_left = grep(search_term)
    #     else:
    #         results_left = grep(search_term, results_left)
        
    #     print()
    #     print('Results:')
    #     for result in results_left:
    #         result = '/'.join(result.split('/')[:-1]) # the very last item is the file containing the data
    #         result = nested_folders_as_string(result)
    #         print(result)

    # for path, folders, _files in os.walk(FOLDER_DONE):
    #     if len(folders) != 0:
    #         continue
        
    #     path = path[len(FOLDER_DONE):]
    #     # print(f'{path=}')
    #     if path.startswith('/'):
    #         path = path[1:]
    #     chars = nested_folders_as_string(path)

    #     print(chars)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    main()
