cd ..\venv\scripts & activate & cd ..\.. & coverage run --source=esofile_reader -m unittest discover -s ./tests -t ./tests -p test_*.py^
 & coverage report --omit esofile_reader\storage\base_storage.py & coverage html --omit esofile_reader\storage\base_storage.py
