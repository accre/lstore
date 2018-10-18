import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lmgmt",
    version="1.0",
    author="Alan Tackett",
    author_email="alan.tackett@vanderbilt.edu",
    description="LServer management tools",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/accre/lstore",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)