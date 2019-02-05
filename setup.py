import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="OPJ",
    version="0.0.1",
    author="Dmitriy Shikhalev",
    author_email="dmitriy.shikhalev@gmail.com",
    description="Ordered Persistent Journal",
    long_description=long_description,
    long_description_content_type="text/бы",
    url="https://github.com/dmitriy-shikhalev/OPJ",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU GPL",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pytest==4.0.2',
    ]
)