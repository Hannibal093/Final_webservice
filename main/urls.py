"""mysite URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

from . import views

app_name="main"

urlpatterns = [
    path('', views.index, name="index"),
    path('news/<int:page>/', views.news_show, name="news_show"),
    path('stock/<int:page>/', views.stock_show, name="stock_show"),
    # path('stock/<str:industry>/<int:page>/', views.stock_list_industry, name="stock_list_industry"),
    path('stock/result/', views.stock_search, name="stock_search"),
    path('single_stock/<str:stock_id>/', views.single_stock, name="single_stock"),
    path('subscribe/<str:stock_id>/', views.subscribe, name="subscribe"),
    path('unsubscribe/<str:stock_id>/', views.unsubscribe, name="unsubscribe"),
    path('login/', views.login_request, name="login"),
    path('logout/', views.logout_request, name="logout"),
    path('register/', views.register, name="register"),
]+ static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

