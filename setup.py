from setuptools import find_packages, setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name="DataManager",
    url="http://github.com/travel-code-sleep/AWS-Data-Manager.git",
    author="Amit Prusty",
    author_email="amit.prusty@edelmandxi.com",
    # Needed to actually package something
    packages=find_packages(),
    # namespace_packages=["AwsDataManager"],
    # Needed dependencies
    install_requires=[
        'pandas == 1.2.4',
        'path == 15.1.2',
        'pyarrow == 3.0.0',
        'boto3 == 1.17.46',
        'python-dotenv == 0.19.1'
    ],
    # *strongly* suggested for in-house use
    version="0.1",
    license="MIT",
    description="Contains all codes for trend engine web application.",
    long_description=open("README.md").read(),
)
