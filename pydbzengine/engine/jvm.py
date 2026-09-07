import logging
import os
import sys
from pathlib import Path

import jpype

logger = logging.getLogger("pydbzengine.jvm")

################# STEP 1  INIT GLOBAL VARIABLES ####################
# Define paths to Debezium Java libraries and configuration directory.
DEBEZIUM_JAVA_LIBS_DIR = Path(__file__).resolve().parent.parent / "debezium" / "libs"
DEBEZIUM_CONF_DIR = (Path(__file__).resolve().parent.parent / "config").as_posix()

# Manually expand jars (JPype doesn't support glob patterns in classpath like pyjnius)
_jars = list(DEBEZIUM_JAVA_LIBS_DIR.glob("*.jar"))
if not _jars:
    raise ImportError(
        "Debezium jar files not found in pydbzengine/debezium/libs/. "
        "Please download the libraries by running the install_libs.sh script first!"
    )

CLASS_PATHS = [str(j) for j in _jars]
CLASS_PATHS.append(DEBEZIUM_CONF_DIR)

# Add current working directory's config folder to classpath if exists
CONFIG_DIR = Path().cwd().joinpath("config")
if CONFIG_DIR.is_dir() and CONFIG_DIR.exists():
    logger.info(f"Adding classpath: {CONFIG_DIR.as_posix()}")
    CLASS_PATHS.append(CONFIG_DIR.as_posix())


################# STEP 2  INIT JPYPE, JVM ####################
def ensure_jvm_started(
    jvm_opts: list[str] | None = None,
    extra_classpaths: list[str] | None = None,
    jvm_path: str | None = None,
) -> None:
    """Ensures the Java Virtual Machine is started with required options and classpaths."""
    if not jpype.isJVMStarted():
        if jvm_path is None:
            jvm_path = jpype.getDefaultJVMPath()

            # jpype.getDefaultJVMPath() returns bytes in some versions/OS
            if isinstance(jvm_path, bytes):
                jvm_path = jvm_path.decode("utf-8")

            # Fix for macOS where getDefaultJVMPath might return the Home directory instead of the lib
            if sys.platform == "darwin" and Path(jvm_path).is_dir():
                # Try common locations specifically for macOS/Corretto
                potential_paths = [
                    Path(jvm_path) / "lib" / "server" / "libjvm.dylib",
                    Path(jvm_path) / "lib" / "libjvm.dylib",
                ]
                for p in potential_paths:
                    if p.exists():
                        jvm_path = str(p)
                        break

        # Load custom JVM options from environment variable or default to standard streaming limits
        resolved_jvm_opts = ["-Xms256m", "-Xmx2g", "-XX:+UseG1GC"]
        env_opts = os.environ.get("PYDBZ_JVM_OPTS")
        if env_opts:
            resolved_jvm_opts.extend(env_opts.split())
        if jvm_opts:
            resolved_jvm_opts.extend(jvm_opts)

        cp = list(CLASS_PATHS)
        if extra_classpaths:
            cp.extend(extra_classpaths)

        jpype.startJVM(jvm_path, *resolved_jvm_opts, classpath=cp)
        logger.info("JVM started.")
