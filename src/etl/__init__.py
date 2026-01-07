"""
ETL Package for Traffic Data Processing
========================================

This file makes the `etl` directory a Python "package" - a collection of modules
that can be imported together. When Python sees a directory with an __init__.py
file, it treats that directory as a package.

WHY THIS FILE EXISTS:
--------------------
1. Package Recognition: Without this file, Python won't recognize `etl/` as a package
2. Package Initialization: Code here runs when you `import etl` or any submodule
3. Public API: You can expose specific functions/classes at the package level
4. Version Management: Convention to define __version__ here

HOW IMPORTS WORK WITH THIS PACKAGE:
-----------------------------------
After running `pip install -e .` (see pyproject.toml), you can import like:

    from etl import __version__           # Gets version from this file
    from etl.config import load_config    # Gets function from config module
    from etl.transform import transform   # Gets function from transform module

The `-e` flag means "editable" - changes to source files take effect immediately
without reinstalling. This is essential during development.

WHY `pip install -e .` IS REQUIRED:
-----------------------------------
When you run `python -m etl`, Python needs to find the `etl` package. There are
two ways this can work:

1. The package is "installed" (exists in site-packages or is registered via pip)
2. The package directory is in PYTHONPATH or sys.path

Running `pip install -e .` tells pip to:
- Read pyproject.toml to understand the package structure
- Register the package so Python can find it from anywhere
- In "editable" mode, link to your source code (not copy it)

Without this, `python -m etl` would fail with "No module named etl" because
Python wouldn't know where to find the package.

ALTERNATIVE: You could also run from the src/ directory or modify PYTHONPATH,
but `pip install -e .` is the standard, clean approach for development.
"""

# Package version - follows semantic versioning (MAJOR.MINOR.PATCH)
# This can be imported as: from etl import __version__
__version__ = "1.0.0"
