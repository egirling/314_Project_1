from setuptools import find_packages, setup

setup(
    name="314Project1DAG",
    packages=find_packages(exclude=["314Project1DAG_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
