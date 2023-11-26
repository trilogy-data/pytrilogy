# from preql.compiler import compile
## set up imports of local module code
import os
nb_path = os.path.abspath("")
from sys import path
from os.path import dirname

path.insert(0,  dirname(nb_path))

# nb_path

from preql.core.models import Select, Grain
from preql.core.query_processor import process_query
from preql.parser import parse
from logging import StreamHandler, INFO
from preql.constants import logger