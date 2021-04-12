#################################
##### Name: Greg Nickel  ########
##### Uniqname: gnickel  ########
#################################

import sqlite3

# Part 1: Read data from a database called choc.db
DBNAME = 'choc.sqlite'

# Part 1: Implement logic to process user commands
def process_command(command):
    """
    none, country, region, DEFALT = None
    sell, source, DEFAULT = Sell
    ratings, cocoa, DEFAULT = Rating
    top, bottom, DEFAULT = top
    <integer> DEFAULT 10
    Parameters:
    ----------
    Command (string)

    Return
    ----------
    Querry Results (tuple)
    """
    connection = sqlite3.connect(DBNAME)
    cursor = connection.cursor()

    parsed = command.split()

    double_parsed = []
    for item in parsed:
        items = item.split("=")
        double_parsed += items

    for item in parsed:
        if item.isnumeric():
            limit = item
        else:
            limit = '10'

    from_arg = 'Bars INNER JOIN Countries sell ON Bars.CompanyLocationId=sell.Id INNER JOIN Countries source ON Bars.BroadBeanOriginId=source.Id'

    if "bottom" in parsed:
        order_direction = 'ASC'
    else:
        order_direction = 'DESC'

    if parsed[0].lower() == 'bars':
        query = bars_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    elif parsed[0].lower() == 'companies':
        query = companies_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    elif parsed[0].lower() == 'countries':
        query = countries_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    elif parsed[0].lower() == 'regions':
        query = regions_query_maker(parsed,double_parsed,limit,from_arg,order_direction)
    else:
        print("Make sure to use the proper keywords")
        query = ""
    print(query)
    result = cursor.execute(query).fetchall()
    connection.close()
    return result

def bars_query_maker(parsed,double_parsed,limit,from_arg,order_direction):
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
    if "source" in parsed:
        selected_fields = 'source.EnglishName, source.Region'
        group_arg = "GROUP BY source.EnglishName"
        having_arg = "HAVING COUNT(source.EnglishName)>=5"
    else:
        selected_fields = 'sell.EnglishName, sell.Region'
        group_arg = "GROUP BY sell.EnglishName"
        having_arg = "HAVING COUNT(sell.EnglishName)>=5"

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

    if 'region' in double_parsed:
        region = double_parsed[double_parsed.index('region')+1]
        where_arg = f"WHERE sell.Region='{region}'"
    else:
        where_arg = ""

    query = f"SELECT {selected_fields} FROM {from_arg} {where_arg} {group_arg} {having_arg} ORDER BY {order_by_arg} {order_direction} LIMIT {limit}"
    return query

def regions_query_maker(parsed,double_parsed,limit,from_arg,order_direction):
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

def load_help_text():
    with open('Proj3help.txt') as f:
        return f.read()

# Part 2 & 3: Implement interactive prompt and plotting. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == 'help':
            print(help_text)
            continue
        results = process_command(response)
        print(results)

# Make sure nothing runs or prints out when this file is run as a module/library
if __name__=="__main__":
    interactive_prompt()
