#!/bin/bash

python -m venv .venv

source .venv/bin/activate

pip install --disable-pip-version-check -r requirements.txt