import setuptools

def _requires_from_file(filename):
    return open(filename).read().splitlines()

# you should update version.
setuptools.setup(
    name="soxai_data",
    version="0.0.2",
    install_requires=_requires_from_file('requirements.txt'),
    packages=["soxai_data"],
)
