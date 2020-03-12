from setuptools import setup, find_packages

setup(
    name="postbox",
    version="0.6",
    packages=find_packages(),
    url="https://github.com/rmcolq/postbox",
    license="MIT",
    entry_points={"console_scripts": ["postbox = postbox.postbox:main"]},
    test_suite="nose.collector",
    tests_require=["nose >= 1.3"],
    install_requires=[
        "numpy>=1.16.1",
        "pandas>=0.24.2",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
    ],
)
