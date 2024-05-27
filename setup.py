from setuptools import find_packages, setup

setup(
    name="tucson_data",
    packages=find_packages(exclude=["tucson_data_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "geopandas",
        "pydantic",
        "requests",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "ipykernel"]},
)
