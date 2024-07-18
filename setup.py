from setuptools import setup, find_packages

setup(
    name="magictables",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "requests",
        "python-dotenv",
    ],
)
