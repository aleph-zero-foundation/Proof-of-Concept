from setuptools import setup, find_packages

setup(
    name="aleph",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "charm-crypto==0.50",
        "pynacl",
        "networkx",
        "numpy",
        "matplotlib",
        "parse",
        "psutil",
        "joblib",
        "pytest-xdist",
        "tqdm"
        ],
    license="",
    package_data={"aleph.test.data": ["simple.dag", "light_nodes_public_keys"]},
)
