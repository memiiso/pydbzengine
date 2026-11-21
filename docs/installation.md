# Installation

## Prerequisites

You must have a **Java Development Kit (JDK) version 17 or newer** installed and available in your system's `PATH`.

## Recommended Installation: From GitHub

!!! warning
    Due to the package size (including `.jar` artifacts), new versions are no longer published to PyPI. The version on PyPI is outdated. Install the package directly from GitHub to get the latest features and fixes.

You can install either the latest development version from the `main` branch or a specific, stable version from a release tag.

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

### Specific Version

To install a specific version from a release tag (e.g., `3.6.3.0`):

```shell
pip install "pydbzengine @ git+https://github.com/memiiso/pydbzengine.git@3.6.3.0"
```

## Alternative: From PyPI (Outdated Version)

An older version is available on PyPI. You can install it, but be aware that it lacks recent features and updates.

```shell
# For core functionality
pip install pydbzengine

# With extras
pip install "pydbzengine[iceberg]"
pip install "pydbzengine[dlt]"
```
