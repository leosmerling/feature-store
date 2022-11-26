import setuptools
import os

version = {}
with open("../../../version.py") as fp:
    exec(fp.read(), version)

setuptools.setup(
    name="leosmerling.feature_store",
    version=version["FEATURES_STORE_APP_VERSION"]
    description="Feature Store App",
    package_dir={
        "": "src"
    },
    packages=[
        "feature_store"
    ],
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=[
        f"hopeit.engine[web,cli,redis-streams,fs-storage,config-manager]>={os.getenv('HOPEIT_ENGINE_VERSION')}",
    ],
    extras_require={
    },
    entry_points={
    }
)
