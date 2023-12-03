import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='oldsnmpagg',
    version='0.0.1',
    author='Victor V. Khodorchenko',
    author_email='v.khodor@@gmail.com',
    description='Shared package of CleverBreeze components',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/vkhodor/cleverbreeze-snmpagg-package',
    project_urls = {
        "Bug Tracker": "https://github.com/vkhodor/cleverbreeze-snmpagg-package/issues"
    },
    license='MIT',
    packages=['oldsnmpagg'],
    install_requires=['oldsnmpagg'],
)
