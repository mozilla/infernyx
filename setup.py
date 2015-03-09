from setuptools import setup, find_packages

setup(
    name='infernyx',
    version='0.1.37',
    packages=['infernyx'],
    url='',
    license='',
    author='tspurway',
    author_email='tspurway@mozilla.com',
    description='Inferno rules for Tiles Project',
    install_requires=['psycopg2', 'inferno', 'boto', 'geoip2', 'ua_parser', 'statsd', 'disco'],
    scripts=['scripts/csdash.py'],
    zip_safe=False,
)
