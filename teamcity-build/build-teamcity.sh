#!/usr/bin/env bash
set -x

# this script is meant to be invoked by one of the build-teamcity-$VERSION.sh scripts

VERSION="${1:-v19.2.5}"

# Set an environment variable used by cockroach/django/creation.py.
export RUNNING_COCKROACH_BACKEND_TESTS=1

# clone django into the repo.
git clone --depth 1 --single-branch --branch cockroach-master https://github.com/timgraham/django _django_repo

# install the django requirements.
cd _django_repo/tests/
pip3 install -e ..
pip3 install -r requirements/py3.txt
pip3 install -r requirements/postgres.txt
cd ../..

# install the django-cockroachdb backend.
pip3 install .

# download and start cockroach
if [ $VERSION == "LATEST" ]
then
    wget "https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST" -O cockroach_exec
    chmod +x cockroach_exec
else
    wget "https://binaries.cockroachdb.com/cockroach-${VERSION}.linux-amd64.tgz"
    tar -xvf cockroach-${VERSION}*
    cp cockroach-${VERSION}*/cockroach cockroach_exec
fi
./cockroach_exec start-single-node --insecure &

cd _django_repo/tests/

# Bring in the settings needed to run the tests with cockroach.
cp ../../teamcity-build/cockroach_settings.py .

# Run the tests!
python3 ../../teamcity-build/runtests.py
