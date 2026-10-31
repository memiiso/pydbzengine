# Installation

## Prerequisites

A **Java Development Kit (JDK) version 17 or newer** must be installed and available in your system `PATH` (or configured via `JAVA_HOME`).

## Installing from GitHub

!!! warning "PyPI Version Notice"
    Due to bundled `.jar` artifact size limits, new releases are not published to PyPI. Install directly from GitHub to obtain current features and fixes.

### Latest Main Branch

```shell
# Core package (includes pyarrow and jpype1)
pip install "git+https://github.com/memiiso/pydbzengine.git"

# With optional integrations
pip install "pydbzengine[iceberg] @ git+https://github.com/memiiso/pydbzengine.git"
pip install "pydbzengine[dlt] @ git+https://github.com/memiiso/pydbzengine.git"
pip install "pydbzengine[airflow] @ git+https://github.com/memiiso/pydbzengine.git"
```

### Specific Release Tag

```shell
pip install "pydbzengine @ git+https://github.com/memiiso/pydbzengine.git@3.4.1.0"
```

## Installing Legacy Version from PyPI

```shell
pip install pydbzengine
pip install "pydbzengine[iceberg]"
pip install "pydbzengine[dlt]"
```
