"""
This file is used to install this package via the pip tool.

It keeps track of versioning, as well as dependencies and
what versions of python we support.
"""
from setuptools import find_packages, setup


setup(
    name="async-rediscache",
    version="1.0.0rc3",
    description="An easy to use asynchronous Redis cache",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Python Discord",
    author_email="staff@pythondiscord.com",
    url="https://github.com/python-discord/async-rediscache",
    license="MIT",
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_data={"async_rediscache": ["py.typed"]},
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Framework :: AsyncIO",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=[
        "redis~=5.0"
    ],
    python_requires='~=3.8',
    extras_require={
        "fakeredis": ["fakeredis[lua]~=2.0"],
    },
    include_package_data=True,
    zip_safe=False
)
