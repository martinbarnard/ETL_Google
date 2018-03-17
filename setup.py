from setuptools import setup
setup(
    name='ELT_Google',
    version='0.1',
    setup_requires=['pytest'],
    description='Extract Transform and Load Test',
    py_modules=['app'],
    url='tbd',
    author='martin barnard',
    author_email='barnard.martin@gmail.com',
    license='Public Domain',
    packages=[''],
    entry_points='''
      [console_scripts]
      etl=ETL_Google.src.app:main
    '''
)
