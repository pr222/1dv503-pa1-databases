# Here I will post the code as we go on with the workshop
# My code here
import mysql.connector
from mysql.connector import errorcode


cnx = mysql.connector.connect(user='root',
                              password='root',
                              # database='cars',
                              # host='127.0.0.1:8889'
                              unix_socket='/Applications/MAMP/tmp/mysql/mysql.sock',)

DB_NAME = 'cars'

cursor = cnx.cursor()


def create_database(cursor, DB_NAME):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Faild to create database {}".format(err))
        exit(1)


def create_table_cars(cursor):
    creat_cars = "CREATE TABLE `cars` (" \
                 "  `car_id` int(11) NOT NULL AUTO_INCREMENT," \
                 "  `brand` varchar(14) NOT NULL," \
                 "  `model` varchar(14) NOT NULL," \
                 "  `transmission` varchar(10) NOT NULL," \
                 "  `model_year` SMALLINT NOT NULL," \
                 "  `engine` varchar(14) NOT NULL," \
                 "  `fuel` varchar(14) NOT NULL," \
                 "  PRIMARY KEY (`car_id`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table cars: ")
        cursor.execute(creat_cars)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def insert_into_cars(cursor):
    insert_sql = ["INSERT INTO cars (brand, model, transmission, model_year, engine, fuel)"
                  "VALUES ('BMW', '520D', 'Automatic', '2020', 'i4', 'Diesel');",
                  "INSERT INTO cars (brand, model, transmission, model_year, engine, fuel)"
                  "VALUES ('Audi', 'S6', 'Manual', '2021', 'v6', 'Petrol');",
                  "INSERT INTO cars (brand, model, transmission, model_year, engine, fuel)"
                  "VALUES ('Tesla', 'Model 3', 'Automatic', '2021', 'Dual Electric', 'Electric');",
                  "INSERT INTO cars (brand, model, transmission, model_year, engine, fuel)"
                  "VALUES ('Mercedes', 'E220', 'Automatic', '2021', 'i4', 'Diesel');"
                  ]

    for query in insert_sql:
        try:
            print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            # Make sure data is committed to the database
            cnx.commit()
            print("OK")


try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exist".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, DB_NAME)
        print("Database {} created succesfully.".format(DB_NAME))
        cnx.database = DB_NAME
        create_table_cars(cursor)
        insert_into_cars(cursor)
    else:
        print(err)


query = "SELECT brand, model, fuel, transmission FROM cars;"

cursor.execute(query)

for (brand, model, fuel, transmission) in cursor:
    print("{:<15} {:<15} Fuel: {:<15} Transmission: {}".format(
        brand, model, fuel, transmission))

print("\n\n")

model_year = 2020
query2 = "SELECT brand, model, fuel, transmission FROM cars WHERE model_year>{}".format(
    model_year)

cursor.execute(query2)
print("| {:<15} | {:<15} | {:<15} | {}".format(
    "brand", "model", "fuel", "transmission"))
print("-"*68)
for (brand, model, fuel, transmission) in cursor:
    print("| {:<15} | {:<15} | {:<15} | {}".format(
        brand, model, fuel, transmission))


print("\n\n - Cars count by fuel:")

query3 = '''SELECT fuel, count(car_id) as count
            FROM cars
            GROUP BY fuel'''
cursor.execute(query3)


print("| {:<15} | {}".format("fuel", "count"))
print("-"*68)
for (fuel, count) in cursor:
    print("| {:<15} | {}".format(fuel, count))


cursor.close()
cnx.close()
