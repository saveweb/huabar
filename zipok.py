import os
import subprocess
import sys

perfix = sys.argv[1]
for file in os.listdir():
    print('skip:', file, end='\r')
    if file.startswith(perfix) and file.endswith('.zip'):
        print(file)
        rtcode = subprocess.call(['7z', 't', file])
        if rtcode != 0:
            print('zip file is broken:', file)
            sys.exit(1)