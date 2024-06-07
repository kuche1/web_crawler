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
def grep(text:str, file_or_folder:str) -> list[str]:

    shell = subprocess.Popen(['grep', '-rIil', '--', text, file_or_folder], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    while True:
        line = shell.stdout.readline()

        if line == '' and shell.poll() is not None:
            return
        
        if line.endswith('\n'):
            line = line[:-1]
        
        if line == '':
            return
        
        yield line

def print_result(file):
    print()

    name = file

    name = name[len(FOLDER_DONE):]

    if name.startswith('/'):
        name = name[1:]

    name = os.path.dirname(name) # get rid of the folder

    name = nested_folders_as_string(name)

    print(termcolor.colored(name, RESULT_COLOR))
    print(termcolor.colored(file, FILE_PATH_COLOR))

def main() -> None:

    initial_search = True
    files_containing_search_terms:list[str] = []

    while True:
        print()
        search_term = input('Enter search term: ')

        print()

        if initial_search:
            initial_search = False

            for file in grep(search_term, FOLDER_DONE):
                files_containing_search_terms.append(file)
                print_result(file)

        else:

            for idx, file in reversed(list(enumerate(files_containing_search_terms))):
                if len(list(grep(search_term, file))) == 0:
                    del files_containing_search_terms[idx]
                else:
                    print_result(file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    main()
