#run acquire from the command line
eval "$(conda shell.bash hook)"
conda activate nycbuswatcher
API_KEY=088886bd-cc48-4d7c-bd8a-498d353d7d13 PYTHON_ENV=production python acquire.py