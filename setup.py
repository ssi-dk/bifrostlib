from setuptools import setup, find_packages

setup(
    name='bifrostlib',
    version='2.1.6',
    description='Datahandling functions for bifrost (later to be API interface)',
    url='https://github.com/ssi-dk/bifrostlib',
    author="Kim Ng, Martin Basterrechea",
    author_email="kimn@ssi.dk",
    packages=find_packages(),
    install_requires=[
        'pymongo', 
        'python-magic', 
        'dnspython', 
        'jsmin', 
        'warlock',
        'pandas',
        'libmagic',
    ],
    package_data={"bifrostlib": ['./schemas/bifrost.jsonc']},
    include_package_data=True
    )
