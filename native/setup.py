"""Build the Sentinel native rate limiter.

    python3 native/setup.py build_ext --inplace

Deliberately plain setuptools + pybind11 rather than CMake/scikit-build: the extension
is three translation units with no external dependencies, and a build the reader can
follow in thirty lines is worth more here than one that scales to a library we do not
have. See DECISIONS.md D-01.
"""

import sys
from pathlib import Path

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup

HERE = Path(__file__).parent
VERSION = "0.1.0"

# -O3 matters: the token bucket is arithmetic in a tight loop and the whole premise of
# this extension is that it is faster than the Python equivalent. Benchmarking a -O0
# build against optimised CPython would be a rigged comparison.
extra_compile_args = ["-O3", "-std=c++17"]
if sys.platform != "win32":
    extra_compile_args.append("-fvisibility=hidden")

ext_modules = [
    Pybind11Extension(
        "sentinel_native",
        sources=[str(HERE / "src" / "bindings.cpp"), str(HERE / "src" / "token_bucket.cpp")],
        include_dirs=[str(HERE / "src")],
        cxx_std=17,
        define_macros=[("SENTINEL_VERSION", f'"{VERSION}"')],
        extra_compile_args=extra_compile_args,
    )
]

setup(
    name="sentinel-native",
    version=VERSION,
    description="Native token-bucket / sliding-window rate limiter for Sentinel Gateway",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
    python_requires=">=3.9",
)
