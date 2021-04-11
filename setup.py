import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dict_to_db",
    version="0.0.6",
    author="poshui",
    author_email="poshui@foxmail.com",
    description="便捷的将dict对象存储到Database[sqlite]中 \nConveniently store the dict object in Database[sqlite]",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wordhui/dict_to_db",
    packages=setuptools.find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'openpyxl',
    ],
)
