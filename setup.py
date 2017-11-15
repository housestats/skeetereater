from setuptools import setup, find_packages

setup(
    name='skeetereater',
    version='0.1',
    author='Lars Kellogg-Stedman',
    author_email='lars@oddbit.com',
    url='https://github.com/larsks/skeetereater',
    packages=find_packages(),
    install_requires=[
        'psycopg2',
        'paho_mqtt',
    ],
    entry_points={
        'console_scripts': [
            'skeeter = skeetereater.main:main',
        ],
    }
)
