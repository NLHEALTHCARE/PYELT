from setuptools import setup

# from make_dist import new_version
from versions import current_version


def readme():
    with open('../README.rst') as f:
        return f.read()


setup(name='pyelt',
      version=current_version[0],
      description='Pyelt is a DDL and ETL framework for creating and filling data vault datawarehouses on a postgress database.',
      long_description=open('README.rst').read(),
      url='https://github.com/NLHEALTHCARE/PYELT',
      author='NL Healthcare Clinics (NLHC), Henk-Jan van Reenen',
      author_email='henk-jan.van.reenen@nlhealthcareclinics.com',
      license='MIT',
      packages=['pyelt', 'pyelt.datalayers', 'pyelt.helpers', 'pyelt.mappings', 'pyelt.process', 'pyelt.sources'],
      install_requires=[
          'sqlalchemy', 'typing'
      ],
      include_package_data=True,
      zip_safe=False,
      # test_suite = 'nose.collector',
      # test_require = ['nose'],
      )