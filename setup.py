"""
This file is used to install this package via the pip tool.

It keeps track of versioning, as well as dependencies and
what versions of python we support.
"""
from setuptools import setup


setup(
    name="async-rediscache",
    version="0.1.2",
    description="An easy to use asynchronous Redis cache",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Python Discord",
    author_email="staff@pythondiscord.com",
    url="https://github.com/python-discord/async-rediscache",
    license="MIT",
    packages=['async_rediscache'],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Framework :: AsyncIO",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=[
        "aioredis>=1"
    ],
    python_requires='~=3.7',
    extras_require={
        "fakeredis": ["fakeredis>=1.3.1"],
    },
    include_package_data=True,
    zip_safe=False
)
