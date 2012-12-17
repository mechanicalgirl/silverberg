import os
from setuptools import setup

def getPackages(base):
    packages = []

    def visit(arg, directory, files):
        if '__init__.py' in files:
            packages.append(directory.replace('/', '.'))

    os.path.walk(base, visit, None)

    return packages


setup(
    name='silverberg',
    version='0.0.1',
    description='Twisted CQL Cassandra Client',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Framework :: Twisted'
    ],
    maintainer='Ken Wronkiewicz',
    maintainer_email='ken.wronkiewicz@rackspace.com',
    license='APL2',
    url='https://github.com/racker/silverberg/',
    long_description=open('README.md').read(),
    packages=getPackages('silverberg'),
    install_requires=['Twisted >= 12.0.0', 'thrift == 0.8.0', 'mock'],
)