from django.conf.urls import url
from one_app import views

url_patterns = [

    url(r'^$',views.users,name='users'),
]
