from setuptools import setup, find_packages

VERSION = '1.3.0'
DOWNLOAD_URL = ('https://github.com/philipbl/python-persistent-queue/archive/'
                '{}.zip'.format(VERSION))
PACKAGES = find_packages(exclude=['tests', 'tests.*'])
REQUIRES = []

setup(
    name='python_persistent_queue',
    version=VERSION,
    license='MIT License',
    author='Philip Lundrigan',
    author_email='philipbl@cs.utah.edu',
    download_url=DOWNLOAD_URL,
    install_requires=REQUIRES,
    packages=PACKAGES,
    include_package_data=True,
    test_suite='tests',
    zip_safe=False,
    url='https://github.com/philipbl/python-persistent-queue',
    description='A persistent queue. It is optimized for peeking at values and'
                ' then deleting them off to top of the queue.',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
