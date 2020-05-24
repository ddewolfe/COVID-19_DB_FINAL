# Dylan DeWolfe
# Covid Research Final Project
# covid_gui.py
# CPSC 408

import tkinter
from tkinter import*
from tkinter import ttk
from PIL import ImageTk, Image
import pandas as pd
import numpy as np
from pandas import DataFrame
import mysql.connector
import sqlalchemy

mydb = mysql.connector.connect(
  host="34.106.102.239",
  user="root",
  password="2421",
  database="auto_covid_data"
)

my_cursor = mydb.cursor(buffered=True)
# __________________________________________________________________ db connection stuff

# testting datbase display records
def Display():
  my_cursor.execute("SELECT * FROM Demo")
  all_rows = my_cursor.fetchall()
  df = DataFrame(all_rows, columns=['Temp', 'DateID', 'StateID', 'Total_Cases', 'Total_Deaths', 'Active_Cases', 'Total_Cases_1m_Pop', 'Total_Deaths_1m_Pop' ,'Total_Tests', 'Total_Tests_1m_pop','isDeleted' ])
  print(df.head(10))

Display()

# -----------------------------------------------

# Home Window in Tk
root = Tk()
root.geometry("200x200")
root.title('COVID-19 Database')

# create title label
title_label = Label(root, text="COVID-19 Database", font= 'Helvetica')
title_label.grid(row=0, column=5,columnspan=5, pady="10")

# -------------------------------------------------------------------------------------------
## Search

def search_records():
  search_records = Tk()
  search_records.geometry("400x400")
  search_records.title('Search Database Records')

  def search_now():
    selected = drop.get()

    searched = search_box.get()

    if selected == "Search by...":
      forgot = Label(search_records, text = "Please Select a drop down option...")
      forgot.grid(row=2, column=0)

    if selected == "State":
      sql_search = "SELECT * FROM Demo WHERE StateID = %s"

    if selected == "Temperature":
      sql_search = "SELECT * FROM Demo WHERE Temp = %s"

    if selected == "Date":
      sql_search = "SELECT * FROM Demo WHERE DateID = %s"


    # sql_search = "SELECT * FROM Demo WHERE StateID = %s"
    name = (searched, )
    results = my_cursor.execute(sql_search, name)
    results = my_cursor.fetchall()
    results_df = DataFrame(results, columns=['Temp', 'DateID', 'StateID', 'Total_Cases', 'Total_Deaths', 'Active_Cases',
                                    'Total_Cases_1m_Pop', 'Total_Deaths_1m_Pop', 'Total_Tests', 'Total_Tests_1m_pop',
                                    'isDeleted'])
    new_results = results_df.head(50)
    if not results:
      result = "Record Not Found......."

    searched_label = Label(search_records, text = new_results)
    searched_label.grid(row=2, column = 0, padx = 10)

  # entry box for search
  search_box = Entry(search_records)
  search_box.grid(row=0, column=1, padx=10, pady=10)
  # Entry box Label search
  search_box_label = Label(search_records, text="Search Records ")
  search_box_label.grid(row=0, column=0, padx=10, pady=10)
  # Entry box search  Button
  search_button = Button(search_records, text="Search records", command=search_now)
  search_button.grid(row=1, column=0, padx=10)

  # # Drop Down Box
  drop = ttk.Combobox(search_records, value=["Search by...", "State", "Temperature", "Date"])
  drop.current(0)
  drop.grid(row=0, column=2)


# end search

#------------------------------------------------------------------------------
# display all records // need to fix button click issus, where it pops without the click.
def list_records():
  list_records_query = Tk()
  list_records_query.geometry("400x800")
  list_records_query.title('COVID-19 Database Records')

  # query db
  my_cursor.execute("SELECT * FROM Demo")
  # result = my_cursor.fetchall()
  all_rows = my_cursor.fetchall()
  df = DataFrame(all_rows, columns=['Temp', 'DateID', 'StateID', 'Total_Cases', 'Total_Deaths', 'Active_Cases',
                                    'Total_Cases_1m_Pop', 'Total_Deaths_1m_Pop', 'Total_Tests', 'Total_Tests_1m_pop',
                                    'isDeleted'])
  results = df.head(50)
  lookup_label = Label(list_records_query, text = results )
  lookup_label.pack()

# end list records
#------------------------------------------------------------------------------
# update

#------------------------------------------------------------------------------
# Delete with combined key of state and date... first need to search for the state and date....

def delete_records():
  delete_records = Tk()
  delete_records.geometry("400x300")
  delete_records.title('Delete Database Records')

  delete_box = Entry(delete_records)
  delete_box.grid(row=0, column=1, padx=10, pady=10)
  # Entry box Label search / state box
  delete_box_label = Label(delete_records, text="Enter the State: ")
  delete_box_label.grid(row=0, column=0, padx=10, pady=10)
  # Entry box Label search / date box
  delete_box2 = Entry(delete_records)
  delete_box2.grid(row=2, column=1, padx=10, pady=10)
  delete_box2_label = Label(delete_records, text="Enter the Date : ")
  delete_box2_label.grid(row=2, column=0, padx=10, pady=10)

  delete_val1  = delete_box.get()
  delete_val2 = delete_box2.get()
  delete_vals = delete_val1 +','+delete_val2

  def delete_records_now():
    my_cursor.execute("UPDATE Demo SET isDeleted = 1  WHERE isDeleted = 0  AND DateID = %s AND StateID = %s",[(delete_val1), (delete_val2)])
    mydb.commit()

  delete_button = Button(delete_records, text="Delete Record", command=delete_records_now)
  delete_button.grid(row=4, column=0, padx=10)




  # delete button

#----------------------
#
# --------------------------------------------------------
# create new record
def create_records():
  create_records_query = Tk()
  create_records_query.geometry("400x500")
  create_records_query.title('Add Database Records')

  # Temperature
  Temp_box = Entry(create_records_query)
  Temp_box.grid(row=0, column=1, padx=10, pady=10)
  # Entry box Label for Temp
  Temp_box_label = Label(create_records_query, text="Temperature: ")
  Temp_box_label.grid(row=0, column=0, padx=10, pady=10)

  # Date / DateID
  Date_box = Entry(create_records_query)
  Date_box.grid(row=2, column=1, padx=10, pady=10)
  # Entry box Label for Date
  Date_box_label = Label(create_records_query, text="Date: ")
  Date_box_label.grid(row=2, column=0, padx=10, pady=10)

  # State / StateID
  State_box = Entry(create_records_query)
  State_box.grid(row=4, column=1, padx=10, pady=10)
  # Entry box Label for Date
  State_box_label = Label(create_records_query, text="State: ")
  State_box_label.grid(row=4, column=0, padx=10, pady=10)

  # Total Cases
  cases_box = Entry(create_records_query)
  cases_box.grid(row=6, column=1, padx=10, pady=10)
  # Entry box Label for Date
  cases_box_label = Label(create_records_query, text="Total Confirmed Cases: ")
  cases_box_label.grid(row=6, column=0, padx=10, pady=10)

  # Total Deaths
  deaths_box = Entry(create_records_query)
  deaths_box.grid(row=8, column=1, padx=10, pady=10)
  # Entry box Label for Date
  deaths_box_label = Label(create_records_query, text="Date: ")
  deaths_box_label.grid(row=8, column=0, padx=10, pady=10)

  # Active Cases
  activecases_box = Entry(create_records_query)
  activecases_box.grid(row=10, column=1, padx=10, pady=10)
  # Entry box Label for Date
  activecases_box_label = Label(create_records_query, text="Number of Active Cases: ")
  activecases_box_label.grid(row=10, column=0, padx=10, pady=10)

  # Total cases by 1 million pop
  cases_1m_box = Entry(create_records_query)
  cases_1m_box.grid(row=12, column=1, padx=10, pady=10)
  # Entry box Label for cases by1 million people
  cases_1m_box_label = Label(create_records_query, text="Cases Per 1 million people: ")
  cases_1m_box_label.grid(row=12, column=0, padx=10, pady=10)

  # Deaths by 1 million
  deaths_1m_box = Entry(create_records_query)
  deaths_1m_box.grid(row=14, column=1, padx=10, pady=10)
  # label for deaths by 1 million
  deaths_1m_box_label = Label(create_records_query, text="Deaths Per 1 million people : ")
  deaths_1m_box_label.grid(row=14, column=0, padx=10, pady=10)
  # Total Tessts
  tests_box = Entry(create_records_query)
  tests_box.grid(row=16, column=1, padx=10, pady=10)
  # Entry box Label for total tests
  tests_box_label = Label(create_records_query, text="Tests: ")
  tests_box_label.grid(row=16, column=0, padx=10, pady=10)
  # Total Tests by 1 million pop
  tests_1m_box = Entry(create_records_query)
  tests_1m_box.grid(row=18, column=1, padx=10, pady=10)
  # Entry box Label for 1 million pop
  tests_1m_box_label = Label(create_records_query, text="Tests Per 1 million people: ")
  tests_1m_box_label.grid(row=18, column=0, padx=10, pady=10)
  # is deleted just equals false....

  tempvals = Temp_box.get()
  datevals = Date_box.get()
  statevals = State_box.get()
  casevals = cases_box.get()
  deathvals = deaths_box.get()
  activevals = activecases_box.get()
  case1mvals = cases_1m_box.get()
  death1mvals = deaths_1m_box.get()
  testsvals = tests_box.get()
  tests1mvals = tests_1m_box.get()



  save_record = Button(create_records_query, text="Save Record")
  save_record.grid(row=20 + 15, column=0, padx=10, pady=10)

  sql_command_new = "INSERT INTO Demo(Temp, DateID, StateID, Total_Cases, Total_Deaths, Active_Cases,Total_Cases_1m_POP, Total_Deaths_1m_POP, Total_Tests, Total_Tests_1m_pop,isDeleted)VALUES('%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s')"

  values_new = (tempvals,datevals, statevals,casevals,deathvals , activevals, case1mvals ,death1mvals,testsvals, tests1mvals , 0)

 # my_cursor.execute(sql_command_new, values_new)


  #sql_command = "INSERT INTO Demo(Temp, DateID, StateID, Total_Cases, Total_Deaths, Active_Cases,Total_Cases_1m_POP, Total_Deaths_1m_POP, Total_Tests, Total_Tests_1m_pop,isDeleted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


  my_cursor.execute(sql_command_new, values_new)


  # Commit the changes to the database
  mydb.commit()

# still need to fix create new record....

#------------------------------------------------------------------------------
# home screen buttons
# Creating buttons for the home screen
# list all
list_records_button = Button(root, text ="Display All Records", command= list_records)
list_records_button.grid(row=1, column=5, padx=10)
#search all
search_records_button = Button(root, text ="Search Records", command=search_records)
search_records_button.grid(row=2, column=5,padx=10)
# # update records
update_records_button = Button(root, text ="Update Records")
update_records_button.grid(row=3, column=5,padx=10)
# # create new record
create_records_button = Button(root, text = "Create New Record", command = create_records)
create_records_button.grid(row=4, column=5,padx=10)
# # delete records
delete_records_button = Button(root, text ="Delete Record", command = delete_records)
delete_records_button.grid(row=5, column=5,padx=10)

#------------------------------------------------------
root.mainloop()

