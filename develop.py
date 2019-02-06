from os.path import dirname, join, realpath
import site
SP_DIR = site.getsitepackages()[0]
PATH = dirname(realpath(__file__))

with open(join(SP_DIR, 's3e.pth'), "w+") as f:
    f.write(PATH)
