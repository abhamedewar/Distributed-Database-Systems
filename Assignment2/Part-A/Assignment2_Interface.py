#
# Assignment2 Interface
#

import imp
import psycopg2
import os
import sys
from threading import Thread

# Do not close the connection inside this file i.e. do not perform openConnection.close()


def fragmentPointsTable(pointsTable):

    partition_list = []
    partition_list.append(' from {} where latitude <= 40.718708 and longitude >= -73.98829'.format(pointsTable))
    partition_list.append(' from {} where latitude > 40.718708 and longitude >= -73.98829'.format(pointsTable))
    partition_list.append(' from {} where latitude <= 40.718708 and longitude < -73.98829'.format(pointsTable))
    partition_list.append(' from {} where latitude > 40.718708 and longitude < -73.98829'.format(pointsTable))

    return partition_list   

def fragmentRectangleTable(rectsTable):

    rect_partition_list = []
    rect_partition_list.append(' from {} where (latitude1 <= 40.718708 and longitude1 >= -73.98829) or \
    (latitude2 <= 40.718708 and longitude1 >= -73.98829) or (latitude1 <= 40.718708 and longitude2 >= -73.98829) \
    or (latitude2 <= 40.718708 and longitude2 >= -73.98829)'.format(rectsTable))
    rect_partition_list.append(' from {} where (latitude1 > 40.718708 and longitude1 >= -73.98829) or \
    (latitude2 > 40.718708 and longitude1 >= -73.98829) or (latitude1 > 40.718708 and longitude2 >= -73.98829) or \
    (latitude2 > 40.718708 and longitude2 >= -73.98829)'.format(rectsTable))
    rect_partition_list.append(' from {} where (latitude1 <= 40.718708 and longitude1 < -73.98829) or \
    (latitude2 <= 40.718708 and longitude1 < -73.98829) or (latitude1 <= 40.718708 and longitude2 < -73.98829) or \
    (latitude2 <= 40.718708 and longitude2 < -73.98829)'.format(rectsTable))
    rect_partition_list.append(' from {} where (latitude1 > 40.718708 and longitude1 < -73.98829) or \
    (latitude2 > 40.718708 and longitude1 < -73.98829) or (latitude1 > 40.718708 and longitude2 < -73.98829) or \
    (latitude2 > 40.718708 and longitude2 < -73.98829)'.format(rectsTable))

    return rect_partition_list    

def createFragmentTable(tablename, fragmentPointsData, cursor):

    fragment_table_name_list = []
    for index in range(len(fragmentPointsData)):
        frag_table_name = tablename + str(index + 1)
        cursor.execute('DROP TABLE IF EXISTS ' + frag_table_name)
        cursor.execute('select * into ' + frag_table_name + fragmentPointsData[index])
        fragment_table_name_list.append(frag_table_name)
    
    return fragment_table_name_list

def performSpatialJoin(fragTablePoints, fragTableRect, openConnection, joins_list, frag_index):

    cur = openConnection.cursor()
    cur.execute('select r.geom, count(*) from {} r join {} p on ST_Contains(r.geom,p.geom) group by r.geom'.format(fragTableRect, fragTablePoints))
    record = cur.fetchall()
    cur.close()
    record.sort(key = lambda x: x[1])
    joins_list[frag_index]  = record


def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    #Implement ParallelJoin Here.
    cur = openConnection.cursor()
    frag_points_data = fragmentPointsTable(pointsTable)
    frag_rectangles_data = fragmentRectangleTable(rectsTable)
    frag_tablename_points = createFragmentTable('pointsFragment', frag_points_data, cur)
    frag_tablename_rect = createFragmentTable('rectFragment', frag_rectangles_data, cur)

    joins_list = [None] * 4
    threads_list = []

    for i in range(4):
        threads_list.append(Thread(target=performSpatialJoin, args=(frag_tablename_points[i],
        frag_tablename_rect[i], openConnection, joins_list, i)))
        
    for thread in threads_list:
        thread.start()
        
    for thread in threads_list:
        thread.join()

    result = []
    count_dict = {}
    for record in joins_list:
        for i, count in record:
            if i not in count_dict:
                count_dict[i] = count
            else:
                count_dict[i] = count_dict[i] + count
    for i in count_dict:
        result.append((i,count_dict[i]))
    result.sort(key = lambda x: x[1])
    cur.execute("DROP TABLE IF EXISTS " + outputTable)
    cur.execute("CREATE TABLE " + outputTable + " (rectgeom geometry , count_points INTEGER)")
    
    insert_values = ','.join(['%s'] * len(result))
    insert_query = "insert into {} (rectgeom, count_points) values {}".format(outputTable, insert_values)
    query = cur.mogrify(insert_query, result).decode('utf8')
    cur.execute(query, query)
    openConnection.commit()
    cur.execute('select count_points from ' +  outputTable + ' order by count_points asc')
    data = cur.fetchall()
    points_count = [str(item[0])+'\n' for item in data]
    
    ptr = open(outputPath,"w")
    ptr.writelines(points_count)
    ptr.close()
    

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


