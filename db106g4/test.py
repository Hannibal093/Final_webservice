import csv

with open("./mysql_setting.csv", newline='') as csvfile:
    mysql_setting = csv.DictReader(csvfile)
    for row in mysql_setting:
        db_name = row['database']
        db_user = row['user']
        db_pwd = row['password']
        db_host = row['host']
        db_port = row['port']
