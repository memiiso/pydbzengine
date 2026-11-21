# Installation

## Prerequisites

You must have a **Java Development Kit (JDK) version 17 or newer** installed and available in your system's `PATH`.

## Install from PyPI

Install the latest stable release from [PyPI](https://pypi.org/project/pydbzengine/):

```shell
# Core engine
pip install pydbzengine

# With optional extras
pip install "pydbzengine[arrow]"    # Required for pydbzengine.schema
pip install "pydbzengine[iceberg]"
pip install "pydbzengine[dlt]"
pip install "pydbzengine[airflow]"
```

## Install from GitHub

!!! note
    If a recent release is missing from PyPI (for example, due to package size limits on bundled `.jar` artifacts) or you need unreleased changes from `main`, you can install directly from GitHub instead.

### Latest Development Version

```shell
# Core engine
pip install "git+https://github.com/memiiso/pydbzengine.git"

# With optional extras
pip install "pydbzengine[arrow] @ git+https://github.com/memiiso/pydbzengine.git"    # Required for pydbzengine.schema
pip install "pydbzengine[iceberg] @ git+https://github.com/memiiso/pydbzengine.git"
pip install "pydbzengine[dlt] @ git+https://github.com/memiiso/pydbzengine.git"
pip install "pydbzengine[airflow] @ git+https://github.com/memiiso/pydbzengine.git"
```

### Specific Release Tag

To install a specific version from a GitHub release tag (e.g., `3.7.1.0`):

```shell
pip install "pydbzengine @ git+https://github.com/memiiso/pydbzengine.git@3.7.1.0"
```
