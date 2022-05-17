# Imports
import csv
import mysql.connector
from mysql.connector import errorcode

# Connection to the database
cnx = mysql.connector.connect(user='root', 
                              password='root',
                              unix_socket='/Applications/MAMP/tmp/mysql/mysql.sock')

DB_NAME='raitaniemi'
# CSV files located in a data-folder in the directory.
PLANETS_CSV='./data/planets.csv'
SPECIES_CSV='./data/species.csv'

cursor = cnx.cursor()

""""
############################################
# 1. QUERIES MADE BY OPTIONS FROM THE MENU #
# 2. DATABASE HANDLING AND PARSING FILES   #
# 3. APPLICATION RUN AND SETUP             #
############################################
"""

"""
#################################################
### 1. QUERIES MADE BY OPTIONS FROM THE MENU  ###
#################################################
"""

#
# Query for all planets and list planet names for the user.
#
def list_all_planets():
  print("Listing all planets...")
  
  # We only need to select/ask for the names.
  cursor.execute("SELECT name FROM planets;")
  res = cursor.fetchall()

  print("-" * 30)
  for row in res:
    print("- " + ''.join(row))
 
#
# Query details of a planet with provided planet name and display results.
# 
def search_planet_details():
  try:
    print("Enter name of a planet:")
    planetName = input()
    
    # Here we want all data from a specific planet.
    cursor.execute("SELECT * FROM planets WHERE name='{}';".format(planetName))
    res = cursor.fetchone()

    # There is probably a better python-way to retrieve and present
    # this information, but this is the best I could come up with
    # for the moment that seems to do the job.
    
    print("-" * 30)
    print("Planet: ", ''.join(str(res[0])))
    print("Rotation period: ", ''.join(str(res[1])))
    print("Orbital Period: ", ''.join(str(res[2])))
    print("Diameter: ", ''.join(str(res[3])))
    print("Climate: ", ''.join(str(res[4])))
    print("Gravity: ", ''.join(str(res[5])))
    print("Terrain: ", ''.join(str(res[6])))
    print("Surface Water: ", ''.join(str(res[7])))
    print("Population: ", ''.join(str(res[8])))
  except:
    print("Something went wrong...")

#
# Query species that are higher than provided height and present the results. 
#  
def search_species():
  print("Enter height: ")
  height = input()
  try:
    # We want name and the height for tuples that fulfills the condition.
    cursor.execute("SELECT name, average_height FROM species WHERE average_height>{};".format(height))
    res = cursor.fetchall()
    
    # In this case I choose to present the tuples directly in the list
    # Without converting the values to strings.
    
    print("-" * 30)
    print("Species with average height over {}".format(height))
    for row in res:
      print("- ", row)
  except:
    print("Something went wrong.")
  
#
# Query for the desired climate for a provided species and present result.
#
def find_likely_climate_for_species():
  print("Enter the name of the species:")
  species = input()
  print("-" * 30)
  print("The most likely desired climate for {} is:".format(species))
  
  # Escape apostrophes if needed.
  """
  I should have made a "santitizer"-function, so that it could be used
  for handling input strings in both planets and species. But for now,
  there will be slight code duplication only handling the apostrophes
  regarding the data in the species table.
  
  The escaping handling for species also assumes that the apostrophe
  always will be between characters and does not take into account if
  a name ends with an apostrophe. So not the most elegant solution for
  a more general approach of handling unknown data.
  
  For any input I also do not handle any variations in capital or
  lowercase letters, or any extra spaces. So all inputs are CASE SENSITIVE.
  """
  hasApostrophe = "'" in species
  if hasApostrophe:
    parts = species.split("'")
    escaped = parts[0] + "\\'" + parts[1]
    species = escaped

  try:
    # Begin querying for the homeworld of the species.
    cursor.execute("SELECT homeworld FROM species WHERE name='{}';".format(species))
    res = cursor.fetchone()
    homeworld = res[0]

    # Check if it is even worth to try continuing with next query.
    if homeworld == None:
      print("Could not find a homeworld for the species.")
      raise Exception()

    # Query for the climate of the homeworld.
    cursor.execute("SELECT climate FROM planets WHERE name='{}';".format(homeworld))
    res = cursor.fetchone()
    climate = res[0]

    # Check result for if there even is a climate present, othervise present the result.
    if climate == None:
      print("Could not find a prefered climate.")
      raise Exception()
    else: 
      print(climate)
      print("-" * 30)
  except:
    # Will show if the Exceptions are raised or if there is any trouble with the querying.
    print("NO ANSWER AVAILABLE.")
  
#
# Query for average lifespan of groups of species-classifications.
#
def average_lifespan_per_classification():
  print("Average lifespan of classifications...")
  try:
    # Get the average for lifespans and group by the classification.
    cursor.execute("SELECT classification, AVG(average_lifespan) FROM species GROUP BY classification;")
    res = cursor.fetchall()
    
    for row in res:
      print("Classification: ", str(row[0]))
      # Round years to one decimal...
      try:
        year = round(row[1],1)
      except:
      # ...or present non-roundable data as-is.
        year = str(row[1])
      print("Average Life: ", year)
      print("*")
  except:
    print("Something went wrong...")

"""
###############################################
### 2. DATABASE HANDLING AND PARSING FILES  ###
###############################################
"""

#
# Create a new database
#
def create_new_database(cursor, DB_NAME):
  try: 
    cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
  except mysql.connector.Error as err:
    print("Could not create the database {}".format(err))
    exit(1)
    
#
# Create and add table of planets to DB. 
#
  """
  For this assingment I only create the table as presented in the file. 
  For a more proper and detailed approach I would separate the columns
  for climate and terrain into their own tables, using name as the key
  to be able to access the data.
  
  I made the name-column the only NOT NULL in the table, assuming that
  values may be NULL at the moment but being able to add data later, in
  order to perserve as much available information as possible.
  
  I do not handle 'gravity' in a special way and leave it only as a string,
  since it is not obvious how the data is supposed to be used.
  """
def create_planets_table():
  planets_table = "CREATE TABLE `planets` (" \
                      " `name` VARCHAR(255) NOT NULL," \
                      " `rotation_period` INT," \
                      " `orbital_period` INT," \
                      " `diameter` INT," \
                      " `climate` VARCHAR(255)," \
                      " `gravity` VARCHAR(255)," \
                      " `terrain` VARCHAR(255)," \
                      " `surface_water` INT," \
                      " `population` BIGINT" \
                      ") ENGINE=InnoDB" 
  try:
    cursor.execute(planets_table)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
      print("Table already exists.")
    else:
      print("ERROR: ", err.msg)
  else:
    print("Planet table OK.")
    
#
# Create and add table of species to DB.
#
  """
  I used the same kind of base-approach for this table as for planets described above.
  
  I would also in this case in a more detailed approach "moved out" the colums with
  several values such as eye_colors etc.
  """
def create_species_table():
  planets_table = "CREATE TABLE `species` (" \
                      " `name` VARCHAR(255) NOT NULL," \
                      " `classification` VARCHAR(255)," \
                      " `designation` VARCHAR(255)," \
                      " `average_height` INT," \
                      " `skin_colors` VARCHAR(255)," \
                      " `hair_colors` VARCHAR(255)," \
                      " `eye_colors` VARCHAR(255)," \
                      " `average_lifespan` INT," \
                      " `language` VARCHAR(255)," \
                      " `homeworld` VARCHAR(255)" \
                      ") ENGINE=InnoDB" 
  try:
    cursor.execute(planets_table)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
      print("Table already exists.")
    else:
      print("ERROR: ", err.msg)
  else:
    print("Species table OK.")
  
#
# Parsing csv-file and populate to the database
#
def parse_planets_to_db(PLANETS_CSV):
  print("Parsing planets to the database...")

  rows = []
  # Read the file.
  with open(PLANETS_CSV, 'r') as file:
    reader = csv.reader(file)
    next(reader) # Skip the first row with column names.
    for row in reader:
      rows.append(row)

  # Remove row if first item/index is not valid. 
  # - A planet must have a name.
  for row in rows:
    if row[0] == "NA":
      rows.remove(row)
    elif row[0] == "N/A":
      rows.remove(row)
   
  base_string_planets = "INSERT INTO planets (" \
                "name, rotation_period, orbital_period, " \
                "diameter, climate, gravity, terrain, " \
                "surface_water, population) VALUES ("
                
  end_string_planets = ");"
  
  """  
  NA, N/A & indefinite values
  
  I chose to change the string in the query depending on "invalid
  values" found in the tables. An alternate approach for columns
  of numerical values could have been to instead check if the value
  could be converted to a number and set NULL if not succeeded.
  
  Again, some code duplication handling planets and species 
  that propably could be better refactored.
  
  """
  
  queries = []
  for row in rows:
    query = base_string_planets
    for i in row:
      if i == "NA":
        query += "NULL, "
      elif  i == "N/A": 
        query += "NULL, "
      else:
        query += "'{}', ".format(i)
    # Remove last comma.
    cut = len(query)
    cut = cut - 2
    query = query[0:cut]

    query += end_string_planets
    queries.append(query)

  for query in queries:
    try: 
      # print("SQL query {}: ".format(query), end='')
      cursor.execute(query)
    except mysql.connector.Error as err:
      print("\n\n******* ERROR! ******* ", err.msg, "\n\n")
    else:
      cnx.commit()
      print("Insert OK.")
      
      
#
# Parsing csv-file and populate to the database
#
def parse_species_to_db(SPECIES_CSV):
  print("Parsing species to the database...")

  rows = []
  # Read the file.
  with open(SPECIES_CSV, 'r') as file:
    reader = csv.reader(file)
    next(reader) # Skip the first row with column names.
    for row in reader:
      rows.append(row)

  # Remove row if first item/index is not valid. 
  # - A species must have a name.
  for row in rows:
    if row[0] == "NA":
      rows.remove(row)
    elif row[0] == "N/A":
      rows.remove(row)
   
  base_string_species = "INSERT INTO species (" \
                "name, classification, designation, average_height, " \
                "skin_colors, hair_colors, eye_colors, " \
                "average_lifespan, language, homeworld) VALUES ("
                
  end_string_species = ");"
  
  queries = []
  for row in rows:
    query = base_string_species
    for i in row:
      if i == "NA":
        query += "NULL, "
      elif  i == "N/A": 
        query += "NULL, "
      elif i == "indefinite":
        query += "NULL, "
      else:
        hasApostrophe = "'" in i
        if hasApostrophe:
          parts = i.split("'")
          escaped = parts[0] + "\\'" + parts[1]
          query += "'{}', ".format(escaped)
        else:
          query += "'{}', ".format(i)
    # Remove last comma.
    cut = len(query)
    cut = cut - 2
    query = query[0:cut]

    query += end_string_species
    queries.append(query)

  for query in queries:
    try: 
      #print("SQL query {}: ".format(query), end='')
      cursor.execute(query)
    except mysql.connector.Error as err:
      print("\n\n******* ERROR! ******* ", err.msg, "\n\n")
    else:
      cnx.commit()
      print("Insert OK.")

"""
#####################################
### 3. APPLICATION RUN AND SETUP  ###
#####################################
"""

#
# Make sure there is a DB is up and running,
# othervise create a new one and populate with data.
#
def prepare_db():
  try:
    cursor.execute("USE {};".format(DB_NAME))
  except mysql.connector.Error as err:
    print("The {} database doesn't exist.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Creating database...")
      create_new_database(cursor, DB_NAME)
      print("Database {} created!".format(DB_NAME))
      cnx.database = DB_NAME
      # Add tables.
      create_planets_table()
      create_species_table()
      # Read files and add to database.
      parse_planets_to_db(PLANETS_CSV)
      parse_species_to_db(SPECIES_CSV)
      print("Database populated.")
    else:
      print(err)

#  
# Run Main Menu and control the application flow.
#
def run_application():
  while True:
    print("*********************************")
    print("**        MAIN MENU")
    print("** [1] List all planets.")
    print("** [2] Search planet details.")
    print("** [3] Search species with height higher than a given number.")
    print("** [4] What us the most likely desired climate of a given species?")
    print("** [5] What is the average lifespan per classification of species?")
    print("** [q] Quit the application.")
    print("*********************************")
    print("Choose an option from the menu:")
    option = input()
    
    if option == "1":
      list_all_planets()
    elif option == "2":
      search_planet_details()
    elif option == "3":
      search_species()
    elif option == "4":
      find_likely_climate_for_species()
    elif option == "5":
      average_lifespan_per_classification()
    elif option == "q":
      break
      
    print("****************************************")
    input("Press ENTER to return to the Main Menu:")      
  
#  
# Close connections
#
def close_application():
  print("Closing application...")
  # cursor.execute("DROP DATABASE {}".format(DB_NAME))
  cursor.close()
  cnx.close()
  print("Application closed.")   
 
#    
# Run all code from here
#
prepare_db()
run_application()
close_application()
