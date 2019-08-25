from datetime import datetime

import mysql.connector
from mysql.connector import Error
import Miner.TwitterMiner as tminer
import csv
from twitter.error import TwitterError as TwitterError


class dbConnection:
    def __init__(self, db, type, host, user, password):
        self.db = db
        self.type = type
        self.host = host
        self.user = user
        self.password = password
        self.conn_status = False
        self.output_file = ''
        self.connect()

    def connect(self):
        try:
            self.conn = mysql.connector.connect(host=self.host,
                                                database=self.db,
                                                user=self.user,
                                                password=self.password)
            if self.conn.is_connected():
                print('Connected to MySQL database')
                self.conn_status = True
                self.cursor = self.conn.cursor()
        except Error as e:
            print(e)
            self.disconnect()

    def disconnect(self):
        self.conn.close()

    def set_outputfile(self, fname):
        self.output_file = fname


# DB Managers

def save_condor_python(pydb, cond_users, cond_links):
    dict_pyid_cid = dict()
    # save users
    # for user in cond_users:
    #     userid = insert_user_python(pydb, user)
    #     print(str(userid))
    for tweet in cond_links:
        tweetid = get_python_tweetid(pydb, tweet)
        if tweetid is None:
            tweetid = insert_tweet(pydb, tweet)
        from_id = get_python_user(pydb, tweet['SourceUuid'])
        if from_id is not None:
            from_id = from_id[0]
        to_id = get_python_user(pydb, tweet['TargetUuid'])
        if to_id is not None:
            to_id = to_id[0]
        mentionid = get_python_mention(pydb, to_id, from_id, tweetid)
        if mentionid is None and from_id is not None and to_id is not None:
            mentionid = insert_mention(pydb, to_id, from_id, tweetid)
        else:
            print('here')


def collect_condor_data(db: dbConnection, dataset):
    ds_id = get_condor_dataset_id(db, dataset)
    condor_users = get_condor_users(db, ds_id)
    condor_links = get_condor_links(db, ds_id)
    return condor_users, condor_links


# Collect SQL Queries
def insert_tweet(db: dbConnection, tweet):
    sql = "INSERT INTO twitter.tweet (alltext, created_at_datetime," \
          "generator, quote_or_rt_text," \
          "quote_count," \
          "retweet_count,favorite_count) " \
          "VALUES (%s, %s," \
          "%s, %s," \
          "%s," \
          "%s, %s)"
    tmp_txt = tweet['content']
    tmp_quote = ''
    if tweet['created_at'] is not None:
        try:
            created_dt = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
        except ValueError:
            created_dt = datetime.strptime(tweet['created_at'][:-2], '%Y-%m-%d %H:%M:%S')
    tmp_txt = tweet['content']
    if tmp_txt is not None:
        if len(tmp_txt) > 200:
            tmp_txt = tmp_txt[:200]
    else:
        tmp_desc = None
    val = (tmp_txt, created_dt, '', '', 0, tweet['retweet_count'], 0)
    db.cursor.execute(sql, val)
    db.conn.commit()
    return db.cursor.lastrowid


def insert_mention(db: dbConnection, to_id, from_id, tweet_id):
    sql = "INSERT INTO twitter.mentions (users_idto, users_idfrom, tweet_idtweet)" \
          "VALUES (%s, %s, %s)"
    val = (to_id, from_id, tweet_id)
    db.cursor.execute(sql, val)
    db.conn.commit()
    return db.cursor.lastrowid


def insert_user_python(db: dbConnection, user):
    # check if it exists
    # insert item
    userid = get_python_userid(db, user)
    if userid is None:
        # insert into python database
        try:
            user_data = tminer.coll_user_info(user['Uuid'])
            sql, val = get_insert_actor_vals(user_data)
            db.cursor.execute(sql, val)
            db.conn.commit()
            userid = db.cursor.lastrowid
        except TwitterError as e:
            print('error searching for ' + user['Uuid'] + ' : ' + e.message[0]['message'])
    else:
        userid = userid[0][0]
    return userid


def get_insert_actor_vals(user):
    sql = "INSERT INTO twitter.users (screen_name, name," \
          "description, followers_count," \
          "friends_count,listed_count," \
          "favorites_count,created_at) " \
          "VALUES (%s, %s," \
          "%s, %s," \
          "%s, %s," \
          "%s, %s)"
    tmp_desc = user.description
    if tmp_desc is not None:
        if len(tmp_desc) > 200:
            tmp_desc = tmp_desc[:200]
    else:
        tmp_desc = None
    if user.created_at is not None:
        created_dt = datetime.strptime(user.created_at, '%a %b %d %H:%M:%S %z %Y')
    else:
        created_dt = None
    val = (user.screen_name, user.name,
           tmp_desc, user.followers_count,
           user.friends_count, user.listed_count,
           user.favourites_count, created_dt)

    return sql, val


def get_condor_links(db: dbConnection, ds_id):
    sql = 'SELECT * from condor_bcp.link WHERE fkIdDataset = %s'
    val = (ds_id,)
    db.cursor.execute(sql, val)
    try:
        # check if it exists
        myresult = db.cursor.fetchall()
        test = myresult[0][0]
    except (mysql.connector.errors.InterfaceError, IndexError):
        print('no links with the data set id ' + str(ds_id))
    return myresult


def get_condor_users(db: dbConnection, ds_id):
    sql = 'SELECT * from condor_bcp.actor WHERE fkIdDataset = %s'
    val = (ds_id,)
    db.cursor.execute(sql, val)
    try:
        # check if it exists
        myresult = db.cursor.fetchall()
        test = myresult[0][0]
    except (mysql.connector.errors.InterfaceError, IndexError):
        print('no accounts with the data set id ' + str(ds_id))
    return myresult


def get_condor_dataset_id(db: dbConnection, dataset):
    ds_id = None
    # get data set id
    sql = 'SELECT id from condor_bcp.dataset WHERE name = %s'
    val = (dataset,)
    db.cursor.execute(sql, val)
    try:
        # if it exists copy the id
        myresult = db.cursor.fetchall()
        ds_id = myresult[0][0]
    except (mysql.connector.errors.InterfaceError, IndexError):
        print('no dataset with the name ' + dataset)
    return ds_id


def get_python_mention(db: dbConnection, to_id, from_id, tweet_id):
    sql = "SELECT idmentions FROM twitter.mentions WHERE users_idto = %s " \
          "AND users_idfrom = %s AND tweet_idtweet = %s"
    val = (to_id, from_id, tweet_id)
    db.cursor.execute(sql, val)
    try:
        # check if it exists
        myresult = db.cursor.fetchall()
        myresult = myresult[0][0]
    except (mysql.connector.errors.InterfaceError, IndexError):
        myresult = None
    return myresult


def get_python_tweetid(db: dbConnection, target):
    if target['created_at'] is not None:
        created_dt = datetime.strptime(target['created_at'][:-2], '%Y-%m-%d %H:%M:%S')
    else:
        created_dt = None
    sql = 'SELECT twitter.tweet.idtweet from twitter.tweet WHERE tweet.alltext = %s ' \
          'AND tweet.created_at_datetime = %s'
    val = (target['content'], created_dt)
    db.cursor.execute(sql, val)
    try:
        # check if it exists
        myresult = db.cursor.fetchall()
        myresult = myresult[0][0]
    except (mysql.connector.errors.InterfaceError, IndexError):
        # print('no user id with the data set id ' + str(target))
        myresult = None
    return myresult


def get_python_userid(db: dbConnection, target):
    sql = 'SELECT twitter.users.idusers from twitter.users WHERE screen_name = %s'
    val = (target['Uuid'],)
    db.cursor.execute(sql, val)
    try:
        # check if it exists
        myresult = db.cursor.fetchall()
        test = myresult[0][0]
    except (mysql.connector.errors.InterfaceError, IndexError):
        # print('no user id with the data set id ' + str(target))
        myresult = None
    return myresult


def get_python_user(db: dbConnection, screen_name):
    ds_id = None
    sql = 'SELECT * from twitter.users WHERE screen_name = %s'
    val = (screen_name,)
    db.cursor.execute(sql, val)
    try:
        # if it exists copy the id
        myresult = db.cursor.fetchall()
        ds_id = myresult[0]
    except (mysql.connector.errors.InterfaceError, IndexError):
        print('no dataset with the name ' + screen_name)
    return ds_id


# Csv Manager
def loadcsv(user_fn, link_fn):
    user_dict_lst = []
    link_dict_lst = []
    with open(user_fn, encoding='utf-8') as csvfile:
        dread = csv.DictReader(csvfile, delimiter=',')
        for line in dread:
            user_dict_lst.append(line)
    with open(link_fn, encoding='utf-8') as csvfile:
        dread = csv.DictReader(csvfile, delimiter=',')
        for line in dread:
            link_dict_lst.append(line)
    return user_dict_lst, link_dict_lst


# Insert Data

# Main Functions
def merge_condor_db(condordb_name, targetdb_name):
    try:
        condordb = dbConnection(condordb_name, 'condordb', '127.0.0.1', 'nthorne', 'testpass')
        pydb = dbConnection(targetdb_name, 'pydb', '127.0.0.1', 'nthorne', 'testpass')
        cond_users, cond_links = collect_condor_data(condordb, 'merge31_07')
        save_condor_python(pydb, cond_users, cond_links)
    except Error as e:
        print(e.msg)
    finally:
        condordb.disconnect()
        pydb.disconnect()


def merge_condor_csv(user_fn, link_fn):
    pydb = dbConnection('twitter', 'pydb', '127.0.0.1', 'nthorne', 'testpass')
    cond_users, cond_links = loadcsv(user_fn, link_fn)
    save_condor_python(pydb, cond_users, cond_links)


if __name__ == '__main__':
    # merge_condor_db('condor_bcp', 'twitter')
    merge_condor_csv('nodes_neg_all.csv', 'links_neg_all.csv')
