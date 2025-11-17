# First run: use .env
ENV_FILE=.env_fms17 coverage run --data-file=.coverage.env_fms17 \
    -m unittest discover -s tests -t tests

## 2nd run: use .env2
ENV_FILE=.env_fms22 coverage run --data-file=.coverage.env_fms22 \
    -m unittest discover -s tests -t tests

coverage combine .coverage.env_fms17 .coverage.env_fms22
coverage report -m
coverage html  # optional: generates htmlcov/index.html
coverage xml -o coverage.xml