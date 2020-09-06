
from rich.logging import RichHandler
import logging
from rich.console import Console
from rich.markdown import Markdown
from rich.syntax import Syntax
from rich.table import Table

# Text/color
console = Console()
console.print('Hello World :heart:', style='bold cyan')
console.print('[i]Hello World[/i][bold cyan]:heart:[/bold cyan]')

# Markdown
with open("test_text.txt") as tt:
    md = Markdown(tt.read())
    console.print(md)

# Syntax (disappointing)

my_function = """
def make_note(statement):
    return 'this is one thing I gotta say : {}'.format(statement)
"""
synt = Syntax(my_function, 'python', theme='monokai')
console.print(synt)

# Tables

table = Table(show_header=True, header_style='bold red')

# Adding colors to your logging

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

log = logging.getLogger("rich")
log.info("Hello, World!")
