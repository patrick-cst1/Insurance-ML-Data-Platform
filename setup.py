"""
Setup script for Insurance ML Data Platform framework
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="insurance-ml-platform",
    version="1.0.0",
    author="Patrick Cheung",
    author_email="patrick@example.com",
    description="Production-Ready Medallion Architecture for ML Workflows on Microsoft Fabric",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourorg/Insurance-ML-Data-Platform",
    packages=find_packages(where="framework"),
    package_dir={"": "framework"},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "flake8>=6.1.0",
            "black>=23.12.1",
        ],
    },
    entry_points={
        "console_scripts": [
            "init-watermark=framework.scripts.initialize_watermark_table:main",
            "delta-maintenance=framework.scripts.delta_maintenance:main",
            "validate-deployment=framework.scripts.validate_deployment:main",
            "run-e2e-test=framework.scripts.run_end_to_end_test:main",
        ],
    },
)
