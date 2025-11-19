from setuptools import setup, find_packages

setup(
    name="ai-tour-guide-ml",
    version="0.1.0",
    description="AI Agentic Tour Guide - ML Service",
    author="AI Agentic Tour Guide Team",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "fastapi",
        "uvicorn",
        "torch",
        "transformers",
        "pandas",
        "numpy",
    ],
)
