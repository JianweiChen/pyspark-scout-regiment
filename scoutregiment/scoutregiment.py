import datetime
import doctest
import functools
import inspect
import io
import itertools
import json
import math
import os
import types
import re
import sys
import time
from pprint import pprint
from termcolor import colored


import numpy as np
import pandas as pd
import pyspark
import pyspark.ml
import rich
import rich.console
import rich.syntax
import yaml
from objprint import op
from pyspark.sql import DataFrame, GroupedData, Row, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.functions import expr as sql_
from pyspark.sql.types import *

class ScoutOption(object):
    _erwin = "erwin"
    _hange = "hange"
    _levi = "levi"
    _armin = "armin"
    def __init__(option):

        option.scout_leader = ScoutOption._levi
        option.scout_member_pattern = "^.*$"
        option.scout_level = 1
option = ScoutOption()

scout_rich_console = rich.console.Console(style="magenta")

def _scout_doc(obj):
    docstring = doctest.script_from_examples(obj.__doc__)
    if option.scout_leader.lower() == ScoutOption._levi:
        docstring = '\n'.join([_ for _ in docstring.split("\n") if not _.startswith("#")])
    docstring_in_syntax = rich.syntax.Syntax(docstring, "python", theme='inkpot')
    scout_rich_console.print(docstring_in_syntax)
def _is_single_quota_doc_mark_line(line):
    return line.strip().startswith("'''") or line.strip().endswith("'''")
def _is_double_quota_doc_mark_line(line):
    return line.strip().startswith('"""') or line.strip().endswith('"""')
def _is_single_quota_one_line_doc(line):
    return line.strip().__len__() > 3 and line.strip().startswith("'''") and line.strip().endswith("'''")
def _is_double_quota_one_line_doc(line):
    
    return line.strip().__len__() > 3 and line.strip().startswith('"""') and line.strip().endswith('"""')
  

def _is_doc_mark_line(line):
    return _is_single_quota_doc_mark_line(line) or _is_double_quota_doc_mark_line(line)

def _is_one_line_doc(line):
    return _is_single_quota_one_line_doc(line) or _is_double_quota_one_line_doc(line)

def _scout_source(obj):
    if option.scout_leader.lower() != ScoutOption._armin:
        return 
    drop = False
    lines = []
    for line in inspect.getsourcelines(obj)[0]:
        if _is_doc_mark_line(line) or _is_one_line_doc(line):
            if not _is_one_line_doc(line):
                drop = not drop
        elif not drop:
             lines.append(line)

    source_code = "".join(lines)
    source_code_in_syntax = rich.syntax.Syntax(source_code, "python", theme="inkpot")
    scout_rich_console.print(source_code_in_syntax)

def _scout_by_dir(obj):
    for _m in dir(obj):
        if _m.startswith("__"):
            pass
        elif re.findall(option.scout_member_pattern, _m):
            _scout_member(_m, getattr(obj, _m))
    

def _scout_member(_m, _o):
    if _o is None:
        return
    print(colored(_m, "green", attrs=['bold']))
    print(colored(type(_o), 'blue'))
    scout(_o)

def scout(obj):
    option.scout_level
    if option.scout_level <= 0:
        return
    if obj is None:
        return 
    print(colored(obj, 'red'))
    option.scout_level -= 1
    _scout(obj)
    option.scout_level += 1

@functools.singledispatch
def _scout(obj):
    _scout_by_dir(obj)
    _scout_doc(obj)
    _scout_source(obj)
    
@_scout.register(types.FunctionType)
@_scout.register(types.MethodType)
def _(obj):
    if hasattr(obj, '__doc__'):
        _scout_doc(obj)
        _scout_source(obj)
@_scout.register(type)
def _(obj):
    _scout_by_dir(obj)
    _scout_doc(obj)
    _scout_source(obj)

@_scout.register(types.ModuleType)
def _(obj):
    _scout_by_dir(obj)
    _scout_doc(obj)
    _scout_source(obj)


# option.scout_leader = ScoutOption._levi
# option.scout_member_pattern = "^e.*$"
scout(pd.DataFrame)
