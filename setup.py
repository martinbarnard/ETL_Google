from setuptools import setup
setup(name='ELT_Google',
    version='0.1',
    description='Extract Transform and Load Test',
    py_modules=[''],
    url='tbd',
    author='martin barnard',
    author_email='barnard.martin@gmail.com',
    license='Public Domain',
    packages=[''],
    entry_points='''
    [console_scripts]
    etl=src.app:main
    '''

)
