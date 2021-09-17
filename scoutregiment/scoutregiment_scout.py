
from scoutregiment.scoutregiment_forest import *

import traceback

def bash_(cmd):
    ret = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        logging.warning(ret.stderr)
    return ret.stdout

def bash(cmd):
    print(bash_(cmd).decode())

def _display(*colored_msg_list):
    _tab_width = scout_context.depth
    _tab_string = '  '*_tab_width
    msg = "  ".join(colored_msg_list)
    _msg = f"{_tab_string}{msg}"
    print(_msg)

SCOUT_DEFAULT_MAX_DEPTH = 1
SCOUT_DEFAULT_LEDADER = 'levi'
SCOUT_DEFAULT_PATTERN = "^.*$"

class ScoutContext(object):
    def __init__(self):
        self.depth = 0
scout_context = ScoutContext()

scout_rich_console = rich.console.Console(style="magenta")

def _scout_doc(obj, **options):
    if not options.get("need_test", False):
        return
    if hasattr(obj, '__doc__') and obj.__doc__:
        docstring = doctest.script_from_examples(obj.__doc__)
        if not options.get("need_test_comment", True):
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

def _scout_source(obj, **options):
    if scout_context.depth > 1:
        return
    if options.get("need_source", True):
        return 
    drop = False
    lines = []
    try:
        for line in inspect.getsourcelines(obj)[0]:
            if _is_doc_mark_line(line) or _is_one_line_doc(line):
                if not _is_one_line_doc(line):
                    drop = not drop
            elif not drop:
                lines.append(line)

        source_code = "".join(lines)
        source_code_in_syntax = rich.syntax.Syntax(source_code, "python", theme="inkpot")
        scout_rich_console.print(source_code_in_syntax)
    except:
        traceback.print_exc()

def _scout_by_dir(obj, **options):
    if not options.get("need_member", True):
        return
    for _m in dir(obj):
        if _m.startswith("__"):
            pass
        elif re.findall(options.get("pattern", SCOUT_DEFAULT_PATTERN).strip(), _m):
            _scout_member(_m, getattr(obj, _m), **options)
    

def _scout_member(_m, _o, **options):
    if _o is None:
        return
    _display(colored(_m, "cyan", attrs=[]), colored(type(_o), 'blue'))
    if scout_context.depth >= options.get("max_depth", SCOUT_DEFAULT_MAX_DEPTH):
        return
    scout(_o)

def scout(obj, **options):
    if obj is None:
        return 
    _display(colored(obj, 'green'))
    scout_context.depth += 1
    _scout(obj, **options)
    scout_context.depth -= 1

@functools.singledispatch
def _scout(obj, **options):
    _scout_by_dir(obj, **options)
    _scout_doc(obj, **options)
    _scout_source(obj, **options)
    
@_scout.register(types.FunctionType)
@_scout.register(types.MethodType)
def _(obj, **options):
    _scout_doc(obj, **options)
    _scout_source(obj, **options)
@_scout.register(type)
def _(obj, **options):
    _scout_by_dir(obj, **options)
    _scout_doc(obj, **options)
    _scout_source(obj, **options)

@_scout.register(types.ModuleType)
def _(obj, **options):
    _scout_by_dir(obj, **options)
    _scout_doc(obj, **options)
    _scout_source(obj, **options)

@_scout.register(property)
def _(obj, **options):
    _display(f"property")

