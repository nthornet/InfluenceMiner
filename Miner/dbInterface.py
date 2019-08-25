import mysql.connector
from mysql.connector import Error


def dbConnect(db):
    """ Connect to MySQL database """
    hostname = '127.0.0.1'
    port = 3306
    username = 'nthorne'
    password = 'testpass'

    """ Connect to MySQL database """
    try:
        conn = mysql.connector.connect(host=hostname,
                                       database=db,
                                       user=username,
                                       password=password)
        if conn.is_connected():
            print('Connected to MySQL database')
        return conn
    except Error as e:
        print(e)

    finally:
        conn.close()


def get_dataset(dsname):

    hostname = '127.0.0.1'
    port = 3306
    username = 'nthorne'
    password = 'testpass'
    db = 'condor_bcp'
    try:
        conn = mysql.connector.connect(host=hostname,
                                       database=db,
                                       user=username,
                                       password=password)
        if conn.is_connected():
            print('Connected to MySQL database')
            cur = conn.cursor()
            #query = "SELECT * FROM condor_bcp.dataset"
            #cur.execute(query)
            query = "SELECT id,deleted FROM condor_bcp.dataset WHERE name =%s"
            cur.execute(query, (dsname,))
            tbl_dataset = cur.fetchall()
            if len(tbl_dataset) == 1 and tbl_dataset[0][1] == 0:

                cur.reset()
                query = "SELECT * FROM condor_bcp.actor where fkIdDataset = %s"
                cur.execute(query, (tbl_dataset[0][0],))
                nodes = cur.fetchall()

                cur.reset()
                query = "SELECT * FROM condor_bcp.link where fkIdDataset = %s"
                cur.execute(query, (tbl_dataset[0][0],))
                links = cur.fetchall()

    except Error as e:
        print(e)

    finally:
        conn.close()
        return nodes,links


if __name__ == '__main__':
    nodes,links = get_dataset('merge31_07')
