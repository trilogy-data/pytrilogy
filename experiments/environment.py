# from preql.compiler import compile
## set up imports of local module code
import os

from sys import path
from os.path import dirname

nb_path = os.path.abspath("")
path.insert(0, dirname(nb_path))

# nb_path
