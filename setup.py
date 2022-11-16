"""Python package building configuration."""

from glob import glob
from os.path import basename, splitext

from setuptools import find_packages, setup
import sys

if sys.version_info < (3,0):
    sys.exit("This package requires python 3.")

PROJECT_URLS = {
    "Documentation": "https://docs.smartcitizen.me/",
    "Source Code": "https://github.com/fablabbcn/smartcitizen-connector",
}

setup(
    name="smartcitizen-connector",
    version="0.1.0",
    description="Python connector to download information collected in SmartCitizen API",
    author="Óscar González",
    license="GNU General Public License v3",
    keywords=['sensors', 'Smart Citizen'],
    long_description = open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/fablabbcn/smartcitizen-connector",
    packages=find_packages("src"),
    package_dir={"": "src"},
    project_urls=PROJECT_URLS,
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities",
        "Natural Language :: English",
    ],
    install_requires=["pydantic", "requests", "pandas", "tzwhere", "urllib3"],
)
