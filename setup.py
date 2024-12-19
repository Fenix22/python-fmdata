from setuptools import setup

with open('README.md', 'r', encoding='utf-8') as ld:
    long_description = ld.read()

setup(
    name='fmdata',
    version='0.0.1',
    python_requires='>=3.8',
    author='Lorenzo De Siena',
    author_email='dev.lorenzo.desiena@gmail.com',
    description='python-fmdata is a wrapper around the FileMaker Data API.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/Fenix22/python-fmdata',
    packages=['fmdata'],
    include_package_data=True,
    install_requires=['requests>=2'],
    extras_require={
        'cloud': ['pycognito>=0.1.4']
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent'
    ]
)
