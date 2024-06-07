#! /usr/bin/env python3

import argparse
import os
from typing import Optional, Literal
import subprocess

import termcolor

from crawler import FOLDER_DONE
from crawler import nested_folders_as_string

RESULT_COLOR:Literal['red'] = 'red'
FILE_PATH_COLOR:Literal['dark_grey'] = 'dark_grey'

# returns files that contain the given text
def grep(text:str, node:str) -> list[str]:
    shell = subprocess.run(['grep', '-rIil', '--', text, node], capture_output=True)
    if shell.returncode != 0:
        return []

    files = shell.stdout.decode().split('\n')
    while '' in files:
        files.remove('')

    return files

def main() -> None:

    initial_search = True
    files_containing_search_terms:list[str] = []

    while True:
        print()
        search_term = input('Enter search term: ')

        if initial_search:
            initial_search = False
            print("compiling initial list...")
            files_containing_search_terms = grep(search_term, FOLDER_DONE)
            print("initial list compiled")
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    main()
