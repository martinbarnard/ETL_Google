from setuptools import setup, find_packages
setup(
    name='ELT_Google',
    version='0.2',
    setup_requires=['pytest', 'pyflakes'],
    description='Extract Transform and Load Test',
    py_modules=['app'],
    url='tbd',
    author='martin barnard',
    author_email='barnard.martin@gmail.com',
    license='Public Domain',
    packages=find_packages(),
    entry_points='''
      [console_scripts]
      etl=ETL_Google.src.app:main
    ''',
    author='Martin Barnard',
    author_email='barnard.martin@gmail.com',
    description='Google example',
    license='GPL',
    keyword='google',


)
