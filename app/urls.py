from . import views,admin
from .admin import admin_site
from django.urls import path

urlpatterns = [
    path('', admin_site.urls) , 
]