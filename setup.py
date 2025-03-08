from setuptools import setup, find_packages

setup(
    name='equipfailpred',
    version='1.1.0',
    packages=find_packages(),
    include_package_data=True,  # Ensure data files are included
    package_data={'equipfailpred': ['model_config.yaml']},
)
# to create package from your module, run this command
# python setup.py sdist