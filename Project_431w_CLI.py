import sys
import psycopg

def get_database_connection():

    connection = None
    try:
        connection = psycopg.connect(
            dbname="CMPSC431Project",
            user="postgres",
            password="123456789",
            host="localhost",
            port="5432"  
        )
        print("Connection to PostgreSQL DB successful")
        return connection
    except psycopg.DatabaseError as error:
        print(f"Database connection error: {error}")
        sys.exit(1)


def get_table_name(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = cursor.fetchall()
    cursor.close()
    return [table[0] for table in tables]


def get_column_name(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'")
    columns = cursor.fetchall()
    cursor.close()
    return [column[0] for column in columns]
    

def insert_data():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")

    table_index = int(input("Select the table you want to insert new data into: "))
    if (table_index < 1 or table_index > len(tables)):
        print("Invalid table selection!")
        return
    
    selected_table = tables[table_index - 1]
    #Since we start the enumerate at 1, but index start at 0.

    columns = get_column_name(connection, selected_table)
    values = []
    print(f"Inserting data into {selected_table}")
    for column in columns:
        value = input(f"Enter value for {column}: ")
        values.append(value)
        
    value_formatting = ', '.join(['%s'] * len(values))
    
    cursor = connection.cursor()
    try:
        insert_query = f"INSERT INTO {selected_table} ({', '.join(columns)}) VALUES ({value_formatting})"
        cursor.execute(insert_query, values)
        connection.commit()
        print("Data inserted successfully.")
    except psycopg.DatabaseError as error:
        print(f"Error Inserting data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def delete_data():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
    
    table_index = int(input("Select the table you want to delete existed data from: "))
    if (table_index < 1 or table_index > len(tables)):
        print("Invalid table selection!")
        return
    
    selected_table = tables[table_index - 1]

    columns = get_column_name(connection, selected_table)
    print(f"Available columns in {selected_table}: {', '.join(columns)}")

    condition = input("Enter the condition for deleting data (like: column_name = value) (Please make sure enter in right format): ")

    cursor = connection.cursor()
    try:
        delete_query = f"DELETE FROM {selected_table} WHERE {condition}"
        cursor.execute(delete_query)
        connection.commit()
        print("Data deleted successfully.")
    except psycopg.DatabaseError as error:
        print(f"Error Deleting data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    
def update_data():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
    
    table_index = int(input("Select the table you want to update existed data from: "))
    if (table_index < 1 or table_index > len(tables)):
        print("Invalid table selection!")
        return
    
    selected_table = tables[table_index - 1]

    columns = get_column_name(connection, selected_table)
    print(f"Available columns in {selected_table}: {', '.join(columns)}")

    update_column = input(f"Enter the column you want to update: ")
    if update_column not in columns:
        print(f"Column {update_column} doesn't include in this table {selected_table}")
        return
    
    new_val = input (f"Enter the data you want to change for {update_column} : ")

    change_condition = input(f"Enter the condition (like: column_name = value) to choose the column {update_column} you want to change (Make sure format is right)")

    cursor = connection.cursor()
    try:
        update_query = f"UPDATE {selected_table} SET {update_column} = {new_val} WHERE {change_condition}"
        cursor.execute(update_query)
        connection.commit()
        print("Data update successfully.")
    except psycopg.DatabaseError as error:
        print(f"Error Updating data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def search_data():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
    
    table_index = int(input("Select the table you want to search existed data from: "))
    if (table_index < 1 or table_index > len(tables)):
        print("Invalid table selection!")
        return
    
    selected_table = tables[table_index - 1]

    columns = get_column_name(connection, selected_table)
    print(f"Available columns in {selected_table}: {', '.join(columns)}")

    condition = input(f"Enter the condition (like: column_name = value) to search in table (Make sure format is right): ")

    cursor = connection.cursor()
    try:
        search_query = f"SELECT * FROM {selected_table} WHERE {condition}"
        cursor.execute(search_query)
        result = cursor.fetchall()
        if (len(result) != 0):
            print("Search result:")
            for row in result:
                print(row)
        else:
            print("There are no result qulified your condition.")
    except psycopg.DatabaseError as error:
        print(f"Error Searching data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def aggregate_functions():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
    
    table_index = int(input("Select the table you want to use aggregate functions: "))
    if (table_index < 1 or table_index > len(tables)):
        print("Invalid table selection!")
        return
    
    selected_table = tables[table_index - 1]

    columns = get_column_name(connection, selected_table)
    print(f"Available columns in {selected_table}: {', '.join(columns)}")

    selected_column = input("Enter the column to aggregate: ")
    if selected_column not in columns:
        print("The column you entered does not exist in the table.")
        return
    
    print("Valid aggregate function: SUM, AVG, COUNT, MIN and MAX")
    choose_function = input("Enter the aggregate function to perform (SUM, AVG, COUNT, MIN, MAX): ")
    if choose_function not in ["SUM", "AVG", "COUNT", "MIN", "MAX"]:
        print("The function you choose is not valid!")
        return
    
    cursor = connection.cursor()
    try:
        aggregate_query = f"SELECT {choose_function}({selected_column}) FROM {selected_table}"
        cursor.execute(aggregate_query)
        result = cursor.fetchone()
        print(f"{choose_function} of {selected_column} is: {result[0]}")
    except psycopg.DatabaseError as error:
        print(f"Error Aggregate data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
    

def sorting():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
    
    table_index = int(input("Select the table you want to sort: "))
    if (table_index < 1 or table_index > len(tables)):
        print("Invalid table selection!")
        return
    
    selected_table = tables[table_index - 1]

    columns = get_column_name(connection, selected_table)
    print(f"Available columns in {selected_table}: {', '.join(columns)}")

    selected_column = input("Enter the column to sort by: ")
    if selected_column not in columns:
        print("The column you entered does not exist in the table.")
        return
    
    sort_order = input("Enter the sorting order (ASC or DESC): ")
    if sort_order not in ["ASC", "DESC"]:
        print("Invalid sorting order, enter 'ASC' or 'DESC'!")
        return 
    
    cursor = connection.cursor()
    try:
        sorting_query = f"SELECT * FROM {selected_table} ORDER BY {selected_column} {sort_order} LIMIT 10"
        cursor.execute(sorting_query)
        result = cursor.fetchall()
        if result:
            print("Sorted results first 10: ")
            for row in result:
                print(row)
        else:
            print("No record in this table or satisfy the requirement.")
    except psycopg.DatabaseError as error:
        print(f"Error Sorting data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()    


def joins():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if not tables:
        print("No table in this database.")
        return
    print("Here are available tables: ")
    for i, table in enumerate(tables,start = 1):
        print(f"{i}. {table}")

    table_1_index = int(input("Select the first table for the join:"))
    if (table_1_index < 1 or table_1_index > len(tables)):
        print("Invalid table selection!")
        return
    table1 = tables[table_1_index - 1]
    
    table_2_index = int(input("Select the second table for the join:"))
    if (table_2_index < 1 or table_2_index > len(tables)):
        print("Invalid table selection!")
        return
    table2 = tables[table_2_index - 1]

    column_1 = get_column_name(connection, table1)
    column_2 = get_column_name(connection, table2)
    print(f"Here are available columns in first table {table1}: {', '.join(column_1)}")
    print(f"Here are available columns in second table {table2}: {', '.join(column_2)}")

    join_condition = input("Please enter the condition you want to make the join (Table1.Key = Table2.Key): ")

    cursor = connection.cursor()
    try:
        join_query = f"SELECT * FROM {table1} INNER JOIN {table2} ON {join_condition} LIMIT 10"
        cursor.execute(join_query)
        result = cursor.fetchall()
        if result:
            print("Join reselt first 10 rows:")
            for row in result:
                print(row)
        else:
            print("No record in the database or no record satisfy join condition.")
    except psycopg.DatabaseError as error:
        print(f"Error Join data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()  


def grouping():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
    
    table_index = int(input("Select the table you want to group data in: "))
    if (table_index < 1 or table_index > len(tables)):
        print("Invalid table selection!")
        return
    
    selected_table = tables[table_index - 1]

    columns = get_column_name(connection, selected_table)
    print(f"Available columns in {selected_table}: {', '.join(columns)}")

    selected_column = input("Enter the column to group by: ")
    if selected_column not in columns:
        print("The column you entered does not exist in the table.")
        return
    
    cursor = connection.cursor()
    try:
        group_query = f"SELECT {selected_column}, COUNT(*) FROM {selected_table} GROUP BY {selected_column} LIMIT 10"
        cursor.execute(group_query)
        result = cursor.fetchall()
        if result:
            print("Grouping reselt first 10 rows:")
            for row in result:
                print(row)
        else:
            print("No record in the database or no record satisfy grouping condition.")
    except psycopg.DatabaseError as error:
        print(f"Error Grouping data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()  


def subqueries():
    connection = get_database_connection()

    tables = get_table_name(connection)
    if (len(tables) == 0):
        print("No Table in The Database.")
        return
    else:
        print("Here are available tables: ")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
    
    table_1_index = int(input("Select the first table you choose: "))
    if (table_1_index < 1 or table_1_index > len(tables)):
        print("Invalid table selection!")
        return
    table_1 = tables[table_1_index - 1]
    
    table_2_index = int(input("Select the other table you choose: "))
    if (table_2_index < 1 or table_2_index > len(tables)):
        print("Invalid table selection!")
        return
    table_2 = tables[table_2_index - 1]

    column_1 = get_column_name(connection, table_1)
    column_2 = get_column_name(connection, table_2)
    print(f"Here are available columns in first table {table_1}: {', '.join(column_1)}")
    print(f"Here are available columns in second table {table_2}: {', '.join(column_2)}")

    first_selected_column = input(f"Enter the first column to use from first table {table_1}: ")
    if first_selected_column not in column_1:
        print("The column you entered does not exist in the first table.")
        return
    second_selected_column = input(f"Enter the second column to use from second table {table_2}: ")
    if second_selected_column not in column_2:
        print("The column you entered does not exist in the second table.")
        return
    
    cursor = connection.cursor()
    try:
        sub_query = f"SELECT {first_selected_column} FROM {table_1} WHERE {first_selected_column} IN (SELECT {second_selected_column} FROM {table_2}) LIMIT 10"
        cursor.execute(sub_query)
        result = cursor.fetchall()
        if result:
            print("Subquery reselt first 10 rows:")
            for row in result:
                print(row)
        else:
            print("No record in the database or no record satisfy subquery condition.")
    except psycopg.DatabaseError as error:
        print(f"Error Subquery data: {error}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()  



def exit_program():
    print("Exiting the program.")
    sys.exit()







functions = {
    '1': insert_data,
    '2': delete_data,
    '3': update_data,
    '4': search_data,
    '5': aggregate_functions,
    '6': sorting,
    '7': joins,
    '8': grouping,
    '9': subqueries,
    '10': exit_program
}

def main():
    while True:
        print("""
Welcome to the Database CLI Interface!

Please select an option:
1. Insert Data
2. Delete Data
3. Update Data
4. Search Data
5. Aggregate Functions
6. Sorting
7. Joins
8. Grouping
9. Subqueries
10. Exit
""")
        choice = input("Enter your choice (1-10): ")
        action = functions.get(choice)
        if action:
            action()
        else:
            print("Invalid choice, please try again.")

if __name__ == '__main__':
    main()
