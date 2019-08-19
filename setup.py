'''
    This is a Proof-of-Concept implementation of Aleph Zero consensus protocol.
    Copyright (C) 2019 Aleph Zero Team
    
    This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

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
