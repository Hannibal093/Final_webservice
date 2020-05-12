from django.shortcuts import render, redirect
from .forms import Newuserform
from django.contrib.auth.forms import AuthenticationForm
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from .mongoconnect import NewsCollection, StockCollection, UserCollection
import re
import pandas as pd


def replacer(s, newstring, index, nofail=False):
    # raise an error if index is outside of the string
    if not nofail and index not in range(len(s)):
        raise ValueError("index outside given string")

    # if not erroring, but the index is still not in the correct range..
    if index < 0:  # add it to the beginning
        return newstring + s
    if index > len(s):  # add it to the end
        return s + newstring

    # insert the new string between "slices" of the original
    return s[:index] + newstring + s[index + 1:]


def index(request):
    if request.user:
        u = UserCollection()
        sub_list = u.get_subscribe(request.user.id)
        if sub_list:
            stock_list = []
            s = StockCollection()
            for i in sub_list:
                stock_list.append(s.get_single_by_id(i))
            print('@')
            return render(request, "main/user_home.html", {"stock":stock_list})
        else:
            print('$')
            return render(request, "main/index.html", {"page": 0}) # should change to recommended stock
    else:
        print('#')
        return render(request, "main/index.html", {"page": 0})


def news_show(request, page=0):
    if page<0:
        page = 0

    if page < 4:
        page_list = [i for i in range(1, 6)]
    else:
        page_list = [i for i in range(page-2, page+3)]

    return render(request, "main/news.html", {"news": NewsCollection().find_twenty(page),
                                              "page_list": page_list,
                                              "page": page,
                                              "last":page-1,
                                              "next":page+1})


def stock_show(request, page=0):
    if page < 0:
        page = 0
    stock = StockCollection().get_stock_list_ftn(page)

    if page < 4:
        page_list = [i for i in range(1, 6)]
    else:
        page_list = [i for i in range(page-2, page+3)]

    return render(request, "main/stock.html", {"stock":stock,
                                               "page_list": page_list,
                                               "page": page,
                                               "last": page - 1,
                                               "next": page + 1})


def stock_search(request):
    query_str = str(request.POST['q'])
    # print(type(query_str))
    if re.search(r'^[0-9]{4}$', query_str) :
        stock_data = StockCollection().get_single_by_id(query_str)
        if not stock_data:
            messages.info(request, f"Your search for {query_str} doesn't exist.")
            return redirect("main:stock_show", page=0)
    elif re.search(r'^[\u4e00-\u9fa5]{2,5}$', query_str):
        stock_data = StockCollection().get_by_name(query_str)
        if not stock_data:
            messages.info(request, f"Your search for {query_str} doesn't exist.")
            return redirect("main:stock_show", page=0)
    else:
        messages.info(request, f"Your search for {query_str} doesn't exist.")
        return redirect("main:stock_show", page = 0)

    stock = {"stock_id": stock_data['stock_id'], "stock_name": stock_data['stock_name']}
    print(stock_data)

    return render(request, "main/stock_search.html", {"stock": [stock]})


def single_stock(request, stock_id):
    s = StockCollection()
    stock_data = s.get_single_by_id(str(stock_id))
    stock_name = stock_data['stock_name']
    stock_industry = stock_data['industry']
    relate_news = s.get_relate_news(stock_id)
    n = NewsCollection()
    news_list = []
    for r in relate_news:
        news_list.append(n.find_by_id(r))
    print(news_list)
    u = UserCollection()
    sub_list = u.get_subscribe(request.user.id)
    if stock_id in sub_list:
        sub = True
    else:
        sub = False

    try:
        price = pd.DataFrame(stock_data['price']).sort_values('date',ascending=False)\
                .head().drop(['成交筆數','成交金額','最後揭示買價','最後揭示賣價'], axis='columns')
        price['date'] = price['date'].dt.strftime("%Y-%m-%d")
        p_row = [list(price.iloc[i]) for i in range(5)]
        p_row.insert(0, list(price.columns))
    except:
        p_row = []
    try:
        month = pd.DataFrame(stock_data['month']).sort_values('date',ascending=False).head(3)\
            .drop(['上月營收','去年當月營收','上月比較增減(%)','前期比較增減(%)'], axis='columns')
        month['date'] = month['date'].dt.strftime("%Y-%m-%d")
        month_row = [list(month.iloc[i]) for i in range(3)]
        month_row.insert(0, list(month.columns))
    except:
        month_row = []
    # try:
    #     season = pd.DataFrame(stock_data['season']).sort_values('date',ascending=False).head(1)
    #     season_row = [list(season.iloc[i]) for i in range(1)]
    #     season_row.insert(0, list(season.columns))
    # except:
    #     season_row = []
    try:
        bargin = pd.DataFrame(stock_data['bargin']).sort_values('date',ascending=False).head(3)\
            .drop(['外陸資買進股數(不含外資自營商)',
                   '外陸資賣出股數(不含外資自營商)',
                   '外資自營商買進股數',
                   '外資自營商賣出股數',
                   '投信買進股數',
                   '投信賣出股數',
                   '自營商買進股數(自行買賣)',
                   '自營商賣出股數(自行買賣)',
                   '自營商買進股數(避險)',
                   '自營商賣出股數(避險)'], axis='columns')
        bargin['date'] = bargin['date'].dt.strftime("%Y-%m-%d")
        bargin_row = [list(bargin.iloc[i]) for i in range(3)]
        bargin_row.insert(0, list(bargin.columns))
    except:
        bargin_row = []
    return render(request, "main/single_stock.html", {"stock_id":stock_id,
                                                      "stock_name":stock_name,
                                                      "stock_industry":stock_industry,
                                                      "subscribed":sub,
                                                      "p_row":p_row,
                                                      "month":month_row,
                                                      # "season":season_row,
                                                      "bargin":bargin_row,
                                                      "news_list":news_list})


# def stock_list_industry(request, industry, page=0):
#     if page < 0:
#         page = 0
#     stock = StockCollection().get_by_industry(industry, page)
#
#     if page < 4:
#         page_list = [i for i in range(1, 6)]
#     else:
#         page_list = [i for i in range(page-2, page+3)]
#
#     return render(request, "main/stock_industry.html", {"stock":stock, "page_list": page_list,
#                                                         "page": page, "last": page - 1,
#                                                         "next": page + 1})


def subscribe(request, stock_id):
    if request.method == "POST":
        u = UserCollection()
        u.subscribe_stock(request.user.id, stock_id)
        return redirect('main:single_stock', stock_id)


def unsubscribe(request, stock_id):
    if request.method == "POST":
        u = UserCollection()
        u.unsubscribe_stock(request.user.id, stock_id)
        return redirect('main:single_stock', stock_id)


def register(request):
    if request.method=="POST":
        form = Newuserform(request.POST)
        form_dict = request.POST
        print(form_dict['username'])
        if form.is_valid():
            user = form.save()
            username = form.cleaned_data.get('username')
            email = form.cleaned_data.get('email')
            user_id = form.cleaned_data.get('id')
            u = UserCollection()
            u.add_user(user_id,username, email)
            messages.success(request, f"New Account Created: {username}")
            login(request, user)
            messages.info(request, f"You are now logged in as {username}")
            return redirect("main:index")
        else:
            for msg in form.error_messages:
                messages.error(request, f"{msg}:{form.error_messages[msg]}")
    form = Newuserform
    return render(request,"main/register.html", {"form": form})


def logout_request(request):
    logout(request)
    messages.info(request, "Logged out successfully! Redirect to Homepage.")
    return redirect("main:index")


def login_request(request):
    if request.method=="POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            user = authenticate(username=username, password=password)
            if user is not None:
                login(request, user)
                messages.info(request,f'You are now logged in as {username}')
                return redirect("main:index")
            else:
                messages.error(request, "Invalid username or password")
        else:
            messages.error(request, "Invalid username or password")
    form = AuthenticationForm()
    return render(request,"main/login.html", {"form": form})
