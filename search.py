#! /usr/bin/env python3

import argparse
import os

from crawler import FOLDER_DONE

def main():
    for path, folders, _files in os.walk(FOLDER_DONE):
        if len(folders) != 0:
            continue
        
        path = path[len(FOLDER_DONE):]
        # print(f'{path=}')
        if path.startswith('/'):
            path = path[1:]
        chars = path.split('/')
        chars = [chr(int(char)) for char in chars]
        chars = ''.join(chars)

        print(chars)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    main()
