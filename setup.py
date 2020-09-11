"""
This file is used to install this package via the pip tool.
It keeps track of versioning, as well as dependencies and
what versions of python or django we support.
"""
from setuptools import find_packages, setup


setup(
    name="async-rediscache",
    version="0.0.1-dev0",
    description="An easy to use asynchronous Redis cache",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Python Discord",
    author_email="staff@pythondiscord.com",
    url="https://github.com/SebastiaanZ/async-rediscache",
    license="MIT",
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=[],
    extras_require={
        "dev": ["flake8"]
    },
    include_package_data=True,
    zip_safe=False
)
