from setuptools import setup, find_packages

setup(
    name="aleph",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "charm-crypto==0.50",
        "coincurve",
        "networkx",
        "numpy",
        "matplotlib",
        ],
    license="",
)
