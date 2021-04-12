#################################
##### Name: Greg Nickel  ########
##### Uniqname: gnickel  ########
#################################

import sqlite3
import plotly.graph_objects as graphy

# Part 1: Read data from a database called choc.db
DBNAME = 'choc.sqlite'

# Part 1: Implement logic to process user commands
def process_command(command):
    """
    Given a command string, parses, checks command for validity and returns querry
    Uses functions to create queries and issue queries
    Parameters:
    ----------
    Command (string)

    Return
    ----------
    Query Results (list of tuples)
    """
    #Parse out Command
    parsed = command.split()
    double_parsed = [] #Double parse splits on '=' to look for region, country info
    for item in parsed:
        items = item.split("=")
        double_parsed += items

    #Check to see if command has too many keywords, too few keywords or improper keyword
    top_level_count = 0
    sell_source_count = 0
    rcn_count = 0
    order_count = 0
    location_count = 0
    bad_command = 0

    for item in parsed:
        if item in ('bars', 'countries', 'companies', 'regions'):
            top_level_count += 1
        elif item in ('source', 'sell'):
            sell_source_count += 1
        elif item in ('ratings', 'cocoa', 'number_of_bars'):
            rcn_count +=1
        elif item in ('top', 'bottom'):
            order_count +=1
        elif item[:8] == 'country=' or item[:7] == 'region=':
            location_count +=1
        elif item == 'barplot':
            pass
        elif item.isnumeric():
            pass
        else: # if the words in the commamand don't fit the previous list, throw a warning
            bad_command = -1
            break

    #If no top level command is given, set bars to be the top level command.
    if top_level_count == 0:
        parsed.insert(0,'bars')
        double_parsed.insert(0,'bars')
        top_level_count = 1

    #If command has proper phrasing, pass to query_complier, otherwise notify user
    list_of_counts = [top_level_count,sell_source_count,rcn_count,order_count,location_count,bad_command]
    if max(list_of_counts) > 1: #Too many of a type of keyword is entered
        print("Too many arguments entered!")
        print(f"Command not recognized: {command}")
        return ""
    elif min(list_of_counts) < 0: #None-keyword was entered into the command
        print("Improper keyword used!")
        print(f"Command not recognized: {command}")
        return ""
    else: #Command passes, heads to query generator
        return query_complier(parsed,double_parsed)

def load_help_text():
    with open('helpme.txt') as f:
        return f.read()

def query_complier(parsed,double_parsed):
    """
    Given parses and double parseds list of commands, issue query. Uses sub-functions to create queries
    Parameters:
    ----------
    parsed (list)
    double_parsed (list)
    Return
    ----------
    Querry Results (tuple)
    """
    connection = sqlite3.connect(DBNAME)
    cursor = connection.cursor()

    #From parsed out results, pull out common SQL arguments, limit, from and order
    limit = '10'
    for item in parsed:
        if item.isnumeric():
            limit = item

    from_arg = 'Bars INNER JOIN Countries sell ON Bars.CompanyLocationId=sell.Id INNER JOIN Countries source ON Bars.BroadBeanOriginId=source.Id'

    if "bottom" in parsed:
        order_direction = 'ASC'
    else:
        order_direction = 'DESC'

    # For top level command, use sub-functions to create specific queries
    if parsed[0] == 'bars':
        query = bars_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    elif parsed[0] == 'companies':
        query = companies_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    elif parsed[0] == 'countries':
        query = countries_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    elif parsed[0] == 'regions':
        query = regions_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    elif parsed[0] == 'exit':
        query = None
        print("Bye!")
    else:
        print("Make sure to use the proper keywords")
    if query is not None:
        result = cursor.execute(query).fetchall()
        connection.close()
        return result
    else:
        return ""

#Specific/TopLevel Query Makers
def bars_query_maker(parsed,double_parsed,limit,from_arg,order_direction):
    """
    Given parses and double parseds list of commands, are common arguments
    return a SQL query for a top level command of "bars"
    Parameters:
    ----------
    parsed (list)
    double_parsed (list)
    limit (string)
    from_arg (string)
    order_direction (string)

    Return
    ----------
    query  (string)
    """
    selected_fields = 'SpecificBeanBarName, Company, sell.EnglishName, Rating, CocoaPercent, source.EnglishName'

    if "cocoa" in parsed:
        order_by_arg = "CocoaPercent"
    else:
        order_by_arg = 'Rating'

    if 'country' in double_parsed:
        alpha2 = double_parsed[double_parsed.index('country')+1]
        if 'source' in parsed:
            where_arg = f"WHERE source.Alpha2='{alpha2}'"
        else:
            where_arg = f"WHERE sell.Alpha2='{alpha2}'"
    elif 'region' in double_parsed:
        region = double_parsed[double_parsed.index('region')+1]
        if 'source' in parsed:
            where_arg = f"WHERE source.Region='{region}'"
        else:
            where_arg = f"WHERE sell.Region='{region}'"
    else:
        where_arg = ""

    query = f"SELECT {selected_fields} FROM {from_arg} {where_arg} ORDER BY {order_by_arg} {order_direction} LIMIT {limit}"
    return query

def companies_query_maker(parsed,double_parsed,limit,from_arg,order_direction):
    """
    Given parses and double parseds list of commands, are common arguments
    return a SQL query for a top level command of "companies"
    Parameters:
    ----------
    parsed (list)
    double_parsed (list)
    limit (string)
    from_arg (string)
    order_direction (string)

    Return
    ----------
    query  (string)
    """

    selected_fields = 'Company, sell.EnglishName'
    group_arg = "GROUP BY Company"
    having_arg = "HAVING COUNT(Company)>=5"

    if 'number_of_bars' in parsed:
        selected_fields += ", COUNT(*) bar_count"
        order_by_arg = "bar_count"
    else:
        if 'cocoa' in parsed:
            selected_fields += ' ,AVG(CocoaPercent)'
            order_by_arg = "AVG(CocoaPercent)"
        else:
            selected_fields += ' ,AVG(Rating)'
            order_by_arg = 'AVG(Rating)'

    if 'country' in double_parsed:
        alpha2 = double_parsed[double_parsed.index('country')+1]
        where_arg = f"WHERE sell.Alpha2='{alpha2}'"
    elif 'region' in double_parsed:
        region = double_parsed[double_parsed.index('region')+1]
        where_arg = f"WHERE sell.Region='{region}'"
    else:
        where_arg = ""

    query = f"SELECT {selected_fields} FROM {from_arg} {where_arg} {group_arg} {having_arg} ORDER BY {order_by_arg} {order_direction} LIMIT {limit}"
    return query

def countries_query_maker(parsed,double_parsed,limit,from_arg,order_direction):
    """
    Given parses and double parseds list of commands, are common arguments
    return a SQL query for a top level command of "countries"
    Parameters:
    ----------
    parsed (list)
    double_parsed (list)
    limit (string)
    from_arg (string)
    order_direction (string)

    Return
    ----------
    query  (string)
    """
    where_arg = ""

    if "source" in parsed:
        selected_fields = 'source.EnglishName, source.Region'
        group_arg = "GROUP BY source.EnglishName"
        having_arg = "HAVING COUNT(source.EnglishName)>=5"
        if 'region' in double_parsed:
            region = double_parsed[double_parsed.index('region')+1]
            where_arg = f"WHERE source.Region='{region}'"
    else:
        selected_fields = 'sell.EnglishName, sell.Region'
        group_arg = "GROUP BY sell.EnglishName"
        having_arg = "HAVING COUNT(sell.EnglishName)>=5"
        if 'region' in double_parsed:
            region = double_parsed[double_parsed.index('region')+1]
            where_arg = f"WHERE sell.Region='{region}'"

    if 'number_of_bars' in parsed:
        selected_fields += ", COUNT(*) as bar_count"
        order_by_arg = "bar_count"
    else:
        if 'cocoa' in parsed:
            selected_fields += ', AVG(CocoaPercent)'
            order_by_arg = "AVG(CocoaPercent)"
        else:
            selected_fields += ', AVG(Rating)'
            order_by_arg = 'AVG(Rating)'

    query = f"SELECT {selected_fields} FROM {from_arg} {where_arg} {group_arg} {having_arg} ORDER BY {order_by_arg} {order_direction} LIMIT {limit}"
    return query

def regions_query_maker(parsed,double_parsed,limit,from_arg,order_direction):
    """
    Given parses and double parseds list of commands, are common arguments
    return a SQL query for a top level command of "regions"
    Parameters:
    ----------
    parsed (list)
    double_parsed (list)
    limit (string)
    from_arg (string)
    order_direction (string)

    Return
    ----------
    query  (string)
    """

    if "source" in parsed:
        selected_fields = 'source.Region'
        group_arg = "GROUP BY source.Region"
        having_arg = "HAVING COUNT(source.Region)>=5"
    else:
        selected_fields = 'sell.Region'
        group_arg = "GROUP BY sell.Region"
        having_arg = "HAVING COUNT(sell.Region)>=5"

    if 'number_of_bars' in parsed:
        selected_fields += ", COUNT(*) bar_count"
        order_by_arg = "bar_count"
    else:
        if 'cocoa' in parsed:
            selected_fields += ', AVG(CocoaPercent)'
            order_by_arg = "AVG(CocoaPercent)"
        else:
            selected_fields += ', AVG(Rating)'
            order_by_arg = 'AVG(Rating)'

    where_arg = ""
    # where_arg += "bar_count >3 "####

    query = f"SELECT {selected_fields} FROM {from_arg} {where_arg} {group_arg} {having_arg} ORDER BY {order_by_arg} {order_direction} LIMIT {limit}"
    return query

#Output related Functions
def make_pretty_output(results):
    """
    Given a tuple of lists, print a pretty table organizing the output.

    Parameter
    ---------
    results (tulple of lists)

    Returns
    ---------
    None
    """
    for row in results:
        for item in row:
            width = len(str(item))

            if isinstance(item,float):
                if item<1:
                    print("{:.0%}".format(item), end="  ")
                else:
                    print("{:.1f}".format(item),end="  ")
            else:
                if width > 12:
                    print(f"{item[:12]}...", end=" ")
                else:
                    print(item, end=" ")
                    print(" "*(14-width), end=" ")
        print(" ")

def bar_graph_maker(results,command):
    """
    Given the results form the query generates a barplot

    Parameters
    ----------
    results (list of tuples)
    command (string)

    Return
    ----------
    None
    """
    parsed = command.split()

    x_values = []
    y_values = []
    if 'companies' in parsed or 'countries' in parsed:
        for result in results:
            x_values.append(result[0])
            y_values.append(result[2])

    elif 'regions' in parsed:
        for result in results:
            x_values.append(result[0])
            y_values.append(result[1])
    else:
        if 'cocoa' in parsed:
            for result in results:
                x_values.append(f"{result[0]} ({result[2]})")
                #Some chocolate bars had the same name, despite being made by diffent companies, this makes for unique names
                y_values.append(result[4])
        else:
            for result in results:
                x_values.append(f"{result[0]} ({result[2]})")
                #Some chocolate bars had the same name, despite being made by diffent companies, this makes for unique names
                y_values.append(result[3])

    bar_data = graphy.Bar(x=x_values, y=y_values)
    basic_layout = graphy.Layout(title=f"Results: {command}")
    fig = graphy.Figure(data=bar_data, layout=basic_layout)
    #fig.show() #just gives an error when i use it, always, so we will write to file and open instead
    fig.write_html("bar.html", auto_open=True)

#Implement interactive prompt and plotting.
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == 'help':
            print(help_text)
            continue
        if response.lower() != 'exit':
            results = process_command(response)
            if len(results) > 0:
                if 'barplot' in response.split():
                    bar_graph_maker(results,response)
                else:
                    make_pretty_output(results)

# Make sure nothing runs or prints out when this file is run as a module/library
if __name__=="__main__":
    interactive_prompt()
