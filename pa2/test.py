
import mysql.connector
from mysql.connector import errorcode
import pandas as pd  # Is used to read the datafiles
import msvcrt as m  # Is used for the function wrait()

# Stores the data from streamingsites.cvs in a variable df that can be used
# to insert the data into MySQL
data = pd.read_csv(r'streamingsites.csv')
df = pd.DataFrame(data)
df = df.astype(object).where(pd.notnull(df), None)

# Stores the data from series.cvs in a variable df2 that can be used
# to insert the data into MySQL
data2 = pd.read_csv(r'series.csv')
df2 = pd.DataFrame(data2)
df2 = df2.astype(object).where(pd.notnull(df2), None)

# Stores the data from has_species.cvs in a variable df3 that can be used
# to insert the data into MySQL
data3 = pd.read_csv(r'has_series.csv')
df3 = pd.DataFrame(data3)
df3 = df3.astype(object).where(pd.notnull(df3), None)

# Conection to mysql
cnx = mysql.connector.connect(user='root',
                              password='root',
                              host='localhost')

DB_NAME = "PA2tineingvarsson"

# Introduces a cursor
cursor = cnx.cursor()


# Function for waiting for pressed buttons, is used in the loop later
def wait():
    m.getch()


# Function for creating a table for the streamingsites in MySQL
def create_table_streamingsites(cursor):
    create_streamingsites = "CREATE TABLE `streamingsites` (" \
                            "  `platform` varchar(20) NOT NULL," \
                            "  `price` INT," \
                            "  `owned_by_user` varchar(10), primary key (platform)" \
                            ") ENGINE=InnoDB"
    try:
        print("Creating table streamingsites: ")

        cursor.execute(create_streamingsites)
        for i, row in df.iterrows():
            if not(row[0] is None):

                sql = "INSERT INTO streamingsites VALUES (%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                cnx.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def create_table_series(cursor):

    create_series = "CREATE TABLE `series` (" \
                    "  `name` varchar(40) NOT NULL," \
                    "  `score` INT," \
                    "  `seasons` INT," \
                    "  `seasons_watched` INT, primary key (name)" \
                    ") ENGINE=InnoDB"

    # Tries to create a table with the name series
    # And if there's already an table with that name, it noifies the user
    try:
        print("Creating table series: ")

        cursor.execute(create_series)
        for i, row in df2.iterrows():
            if not(row[0] is None):

                sql = "INSERT INTO series VALUES (%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                cnx.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


def create_table_has_series(cursor):
    create_has_series = "CREATE TABLE `has_series` (" \
                        "  `platform` varchar(20) NOT NULL," \
                        "  `series` varchar(40) NOT NULL," \
                        "  `seasons_on_platform` INT " \
                        ") ENGINE=InnoDB"

    # Tries to create a table with the name 'has_series'
    # And if there's already an table with that name, it noifies the user
    try:
        print("Creating table has_series: ")

        cursor.execute(create_has_series)
        for i, row in df3.iterrows():
            if not(row[0] is None):

                sql = "INSERT INTO has_series VALUES (%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                cnx.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


# Checks if the database with the name exists
# And if it doesn't exist, it tries to create it.
try:
    cursor.execute("USE {}".format(DB_NAME))
    print("Database {} exist".format(DB_NAME))

except mysql.connector.Error as err:
    print("Database {} does not exist".format(DB_NAME))
    print("Creating database {} ".format(DB_NAME))

    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
        cnx.database = DB_NAME
        create_table_streamingsites(cursor)
        cnx.database = DB_NAME
        create_table_series(cursor)
        cnx.database = DB_NAME
        create_table_has_series(cursor)

        # Creating a view with all information in the database
        # So that it can be used several times.

        # Start by joining two of the tables.
        query = "CREATE VIEW test as SELECT has_series.series,has_series.platform, has_series.seasons_on_platform, streamingsites.price, streamingsites.owned_by_user FROM  has_series JOIN streamingsites ON has_series.platform = streamingsites.platform"
        cursor.execute(query)
        # Divide the code in parts for clarity
        text = "series.name, series.score, series.seasons, series.seasons_watched, test.platform, test.seasons_on_platform, test.price, test.owned_by_user"
        query2 = "CREATE VIEW overview as SELECT {} FROM series LEFT JOIN test ON test.series=series.name;".format(text)
        cursor.execute(query2)

        # query = "CREATE VIEW overview as SELECT series.name, series.seasons_watched, has_series.platform, has_series.seasons_on_platform FROM series JOIN has_series ON series.name=has_series.series WHERE has_series.platform IN (SELECT platform FROM streamingsites WHERE owned_by_user='yes') ORDER BY has_series.platform ASC"
        # cursor.execute(query)

    except mysql.connector.Error as err:
        print("Failed to create database {}".format(err))
        exit(1)

# Function for adding more series to the database


def add_new_series():
    None


def add_new_streamingsite(input_platform="", input_price="", owned_by_user=""):
    if input_platform == "":
        input_platform = input("What is the name of the platform? ")
    if input_price == "":
        input_price = input("How much does this streamingsite cost? ")
    if owned_by_user == "":
        owned_by_user = input("Are you subscribed to this streamingsite? ")
    query = "INSERT INTO streamingsites VALUES ('{}','{}','{}');".format(input_platform, input_price, owned_by_user)
    cursor.execute(query)


# Loop where the user can press different buttonss
# and choose what informations to see

while True:
    print("\nFeatures:")

    print("1. See all watched TV-series")
    print("2. See what's new on all the streamingsites that you have")
    print("3. Overview of all streamingsites in the database")
    print("4. Check which streaming site that is the best accoring to you!")
    print("5. See your five most liked series and where to watch them")
    print("6. Search for a TV-series 🔍")
    print("7. Add or change something!")
    
    print("Q. Quit")
    print("---------")
    user_input = input("Please choose one option: ")

    # Breaks the loop if the user presses the letter Q/q
    if user_input.lower() == "q":
        break

    elif user_input == "1":
        # Since the number of seasons watched will be zero
        # if the user hasn't seen a TV-series
        query = "SELECT name,score FROM series WHERE seasons_watched!='0' ORDER BY name ASC;"
        cursor.execute(query)
        print("List of all TV-series you have watched:")

        print("| {:<35} | {:<10}".format("name", "score"))
        print("-"*50)
        for (name, score) in cursor:
            print(f"| {str(name):<35} |"+score*"⭐")

    elif user_input == "2":
        query = "SELECT series.name, series.seasons_watched, has_series.platform, has_series.seasons_on_platform FROM series JOIN has_series ON series.name=has_series.series WHERE has_series.platform IN (SELECT platform FROM streamingsites WHERE owned_by_user='yes') ORDER BY has_series.platform ASC;"
        cursor.execute(query)
        print() 
        print("TV-series with seasons that you haven't watched on the streamingsites that you pay for.")
        print("Titles in CAPS are series that you never have watched: ")

        streamingsites = []
        for (name, seasons_watched, platform, seasons_on_platform) in cursor:
            if platform not in streamingsites:
                print()
                print(platform.upper())

                print("| {:<35} | {:<15} | {}".format("name:", "new seasons", "total seasons avalible"))
                print("-"*80)
                streamingsites.append(platform)

            if seasons_on_platform > seasons_watched:
                if seasons_watched == 0:
                    print("| {:<35} | {:<15} | {}".format(name.upper(), seasons_on_platform-seasons_watched, seasons_on_platform))
                else:
                    print("| {:<35} | {:<15} | {}".format(name, seasons_on_platform-seasons_watched, seasons_on_platform))

    elif user_input == "3":
        sum = 0
        query = "SELECT SUM(price) FROM streamingsites WHERE owned_by_user='yes';"
        cursor.execute(query)
        for i in cursor:
            sum = i[0]

        print("\nYou currently pay {} kronor per month.".format(sum))
        query2 = "SELECT * FROM streamingsites ORDER BY owned_by_user DESC, price;"
        cursor.execute(query2)

        temp = []
        for (name, price, owned) in cursor:
            if owned not in temp:

                if owned == "yes":
                    print("\nStreamingsites that you already have:")
                    print("-"*37)
                    temp.append(owned)
                else:
                    print("\nStreamingsites that you don't have:")
                    print("-"*37)

                    temp.append(owned)

            print("| {:<20} | {}".format(name, str(price)+" kr"))

    elif user_input == "4":
        print("\nList of the average score of a TV-series from a specific streaming site:")
        print("| {:<35} | {:<10}".format("Steaming site", "average score"))
        print("-"*50)
        best_site = ""
        best_score = 0
        best = ""
        platforms = []
        query = "SELECT DISTINCT platform FROM has_series ORDER BY platform;" 
        cursor.execute(query)
        for (platform) in cursor:
            platforms.append(platform[0])
        for platform in platforms:
            query = "SELECT AVG(score) FROM series WHERE name IN (SELECT series FROM has_series WHERE platform='{}') ;".format(platform)
            cursor.execute(query)

            for score in cursor:
                score = round(score[0],2)

                if score >= best_score:
                    best_site = platform
                    best_score = score
                print(f"| {str(platform):<35} |{str(score):<15}")
        print("Which makes the best streaming site {} with a average score of {}".format(best_site, best_score))


    elif user_input == "5":

        # Since the number of seasons watched will be zero
        # if the user hasn't seen a TV-series
        query = "SELECT series.name, series.score, has_series.platform FROM series LEFT JOIN has_series ON series.name = has_series.series ORDER BY score DESC LIMIT 5;"
        cursor.execute(query)
        print("\nList of your most liked TV-series:\n")

        print(" {:<3} | {:<34} | {:<11} | {:<10}".format("", "name", "platform", "score"))
        print("-"*68)
        i = 0
        for (name, score, platform) in cursor:
            i += 1
            rank = str(i)+"."
            print(f" {rank:<3} |{str(name):<35} |{str(platform):<13}| "+score*"⭐")

    elif user_input == "6":
        user_input = input("Which TV-series would you like to see information about: ")

        query = "SELECT * FROM overview WHERE name LIKE '{}' ORDER BY name ASC;".format("%" + user_input + "%")
        cursor.execute(query)
        print("RESULTS:")

        print("| {:<35} | {:<15} | {:<15} | {:<15} | {:<15} | {:<20} | {}".format("name", "score", "seasons", "seasons_watched", "platform", "seasons_on_platform", "price"))
        print("-"*150)

        for (name, score, seasons, seasons_watched, platform, seasons_on_platform, price, owned) in cursor:
            if type(score) is int:
                score = int(score)*"* "
            else:
                score = ""
            if owned == "yes":
                price = "FREE"

            print(f"| {str(name):<35} | {str(score):<15} | {str(seasons):<15} | {str(seasons_watched):<15} | {str(platform):<15} | {str(seasons_on_platform):<20} | {str(price):<15} ")
            print("-"*150)

    elif user_input == "7":
        # Add something to the database
        print("What would you like to add")
        print("1. Add or change a series")
        print("2. Add or change a streamingsite")
        print("M. Main menu")
        user_input = input("Please choose one option ")

        if user_input == "1":
            print("")
            all_names = []
            input_name = input("What's the name of the series? ").lower()
            query = "SELECT name FROM series;"
            cursor.execute(query)
            for name in cursor:
                all_names.append(name[0])

            if input_name in all_names:

                print("It seems like this series is already in the database")
                user_input = input("Would you like to change something about it? (yes/no) ")
                if user_input.upper() in ["YES", "Y", "1", "CONTINUE", "ACCEPT"]:
                    print("Great!!!!!")
                    query = "SELECT score,seasons,seasons_watched FROM series WHERE name='{}';".format(input_name)
                    cursor.execute(query)
                    for score, seasons, seasons_watched in cursor:
                        input_seasons = input("What is the number of seasons? (The current amount is {}) ".format(seasons))
                        input_seasons_watched = input("How many seasons have you watched? (The current amount is {}) ".format(seasons_watched))
                        input_score = input("What score would you give the series? (The current score is {}) ".format(score))
                        query = "UPDATE series SET score='{}',seasons='{}',seasons_watched='{}' WHERE name='{}';".format(input_score, input_seasons, input_seasons_watched, input_name)
                        cursor.execute(query)

                        query = "SELECT platform,seasons_on_platform FROM has_series WHERE series='{}';".format(input_name)
                        cursor.execute(query)
                        for platform, seasons_on_platform in cursor:
                            print("It seems like {} has this series".format(platform))
                            input_seasons_on_platform = input("How many seasons are available there? The current amount is {}(Leave blank if you don't know) ".format(seasons_on_platform))
                            if input_seasons_on_platform != "":
                                query = "UPDATE has_series SET seasons_on_platform='{}' WHERE platform='{}' and series='{}';".format(input_seasons_on_platform, platform, input_name)
                                cursor.execute(query)
                                input_own = input("Do you own this streamingsite? ")
                                query = "UPDATE streamingsites SET owned_by_user='{}' WHERE platform='{}';".format(input_own, platform)
                                cursor.execute(query)


                        input_platform = input("Is there another streamingsite where you could watch this series?(Leave blank if you don't know)")
                        if input_platform!="":

                            query2 = "SELECT COUNT(platform) FROM streamingsites where platform='{}';".format(input_platform)
                            cursor.execute(query2)
                            for num in cursor:
                                if num[0] >= 1:
                                    print("It is in the database")
                                    # add_new_streamingsite(input_platform,)
                                    # None
                                    input_seasons_on_platform = input("How many seasons are available there? ")
                                    query = "INSERT INTO has_series VALUES ('{}', '{}','{}');".format(input_platform, input_name, input_seasons_on_platform)
                                    cursor.execute(query)
                                    input_own = input("Do you own this streamingsite? ")
                                    query = "UPDATE streamingsites SET owned_by_user='{}' WHERE platform='{}';".format(input_own, input_platform)
                                    cursor.execute(query)

                                else:

                                    print("It is not in the database")
                                    add_new_streamingsite(input_platform)
                                    input_seasons_on_platform = input("How many seasons are available there? ")
                                    query = "INSERT INTO has_series VALUES ('{}', '{}','{}');".format(input_platform, input_name, input_seasons_on_platform)
                                    cursor.execute(query)
                                    if input_platform != "":
                                        input_seasons_on_platform = input("How many seasons are avaliable there?")
                                        query = "INSERT INTO has_series VALUES ('{}','{}','{}');".format(input_platform, input_name, input_seasons_on_platform)
                                        cursor.execute(query)

            else:

                input_seasons = input("How many seasons does this TV-series have? ")
                input_seasons_watched = input("How many seasons have you watched? ")
                input_score = input("What score would you give the series? (Has to be a number) ")
                query = "INSERT INTO series VALUES ('{}', '{}','{}','{}');".format(input_name, input_score, input_seasons, input_seasons_watched)
                cursor.execute(query)
                input_platform = input("Where can you watch the series? (leave blank if you don't know) ")
                if input_platform != "":
                    query2 = "SELECT COUNT(platform) FROM streamingsites where platform='{}';".format(input_platform)
                    cursor.execute(query2)
                    for num in cursor:
                        if num[0] >= 1:
                            print("It is in the database")
                            # add_new_streamingsite(input_platform,)
                            # None
                            input_seasons_on_platform = input("How many seasons are available there? ")
                            query = "INSERT INTO has_series VALUES ('{}', '{}','{}');".format(input_platform, input_name, input_seasons_on_platform)
                            cursor.execute(query)

                        else:
                            print("It is not in the database")
                            add_new_streamingsite(input_platform)
                            input_seasons_on_platform = input("How many seasons are available there? ")
                            query = "INSERT INTO has_series VALUES ('{}', '{}','{}');".format(input_platform, input_name, input_seasons_on_platform)
                            cursor.execute(query)  
                    # if input_platform == "":
                    #     None
                    # else:
                    #     input_seasons_on_platform = input("How many seasons are available there? ")

        if user_input == "2":
            all_streamingsites = []
            input_platform = input("What is the name of the platform? ").lower()
            query = "SELECT platform FROM streamingsites;"
            cursor.execute(query)
            for platform in cursor:
                all_streamingsites.append(platform[0])

            if input_platform in all_streamingsites:

                print("It seems like this streamingsite is already in the database")
                user_input = input("Would you like to change something about it? (yes/no) ")
                if user_input.upper() in ["YES", "Y", "1", "CONTINUE", "ACCEPT"]:
                    print("Great!!!!!")
                    # UPDATE INSTEAD
                    input_price = input("How much does this streamingsite cost? ")
                    owned_by_user = input("Are you subscribed to this streamingsite? ")
                    query = "UPDATE streamingsites SET owned_by_user='{}', price='{}' WHERE platform='{}';".format(owned_by_user, input_price, input_platform)
                    cursor.execute(query)

            else:
                add_new_streamingsite(input_platform)

        cnx.commit()

    # It notifies the user if the input isn't Q/q/1/2/3/4/5/6
    else:
        print("Enter a valid command")
    # After anything has happend it waits until the user presses any button
    wait()
cursor.close()
cnx.close()
