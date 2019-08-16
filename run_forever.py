import os
import sys
import time


def main():
    while True:
        my_dir = os.path.dirname(__file__)
        os.system('%s %s' % (sys.executable, os.path.join(my_dir, 'fim_harvester.py')))
        time.sleep(600)


if __name__ == '__main__':
    main()

