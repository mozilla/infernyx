from setuptools import setup, find_packages

setup(
    name='infernyx',
    version='0.1.24',
    packages=['infernyx'],
    url='',
    license='',
    author='tspurway',
    author_email='tspurway@mozilla.com',
    description='Inferno rules for Tiles Project',
    install_requires=['inferno', 'boto', 'geoip2', 'ua_parser', 'statsd'],
    zip_safe=False,
)
