import setuptools

setuptools.setup(
    name="nbis-pipeline-npc-ena",
    version="0.0.1",
    author="Wolmar Nyberg Åkerström",
    author_email="data@nbis.se",
    description="A small example package",
    long_description="",
    long_description_content_type="text/markdown",
    url="https://github.com/NBISweden/pipeline-npc-ena",
    packages=setuptools.find_packages(where='src'),
    package_dir={"":"src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=['requests'],
)