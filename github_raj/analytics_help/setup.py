from setuptools import setup, find_packages

setup(
    name='ae-measure',
    version='1.0.12-SNAPSHOT',
    description='Attribute Extraction Measuring',
    url='https://gecgithub01.walmart.com/mgariki/ae-measure.git',
    download_url='http://gec-maven-nexus.walmart.com/nexus/content/repositories',
    author='Manoj Garikiparthi',
    author_email='mgarikiparthi@walmartlabs.com',
    license='proprietary',
    classifiers=[
        'groupId :: com.walmart.qarth',
        'artifactId :: ae-measure',
        'releaseId :: labs_releases',
        'snapshotId :: labs_snapshots',
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 2.7',
        'Operating System :: POSIX :: Linux',
    ],
    install_requires=[
        'Flask==0.10.1',
        'requests==2.12.3',
        'beautifulsoup4==4.3.2',
        'backoff==1.4.3',
        'cassandra-driver',
        'numpy',
        'scipy==0.18.1',
        'scikit-learn==0.16.1',
        'Pattern==2.6',
        'pandas==0.17.1',
        'werkzeug'

    ],

    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
)
