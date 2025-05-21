@echo on
call .venv\Scripts\activate
start http://127.0.0.1:8000/app/orders/
python manage.py runserver 0.0.0.0:8000
pause 