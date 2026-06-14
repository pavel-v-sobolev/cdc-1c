
rm -rf .venv 
rm uv.lock
#rm .python-version
uv venv --python python3.14
uv sync --extra dev --editable --upgrade
