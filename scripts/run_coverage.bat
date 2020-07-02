cd ..\.venv\scripts & activate & cd ..\.. & coverage run -m unittest discover -s ./tests -t ./tests -p test_*.py^
 & coverage report  & coverage html
