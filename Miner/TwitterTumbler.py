import mysql.connector
from mysql.connector import Error
import networkx as nx
from datetime import datetime
import Miner.DbMerger as dbTools
from collections import Counter


def create_graph(filename):
    try:
        pydb = dbTools.dbConnection('twitter', 'pydb', '127.0.0.1', 'nthorne', 'testpass')
        G = nx.DiGraph()
        G = get_nodes(pydb, G)
        G = get_links(pydb, G)
        nx.write_gexf(G, filename + ".gexf")
        print('Listo')
    except Error as e:
        print(e)
    finally:
        pydb.disconnect()


def get_nodes(db:dbTools.dbConnection, G):
    tweet_id = None
    user_ment_bool = False

    sql, val = get_actors_sql()
    db.cursor.execute(sql, val)
    myresult = db.cursor.fetchall()
    for user in myresult:
        user = list(user)
        if user[4] is None:
            G.add_node(user[0], screen_name=user[1], nombre=user[2],
                       followers=0, friends=0,
                       favoritos=0, creado=0)
        else:
            G.add_node(user[0], screen_name=user[1], nombre=user[2],
                       followers=user[4], friends=user[5],
                       favoritos=user[7], creado=user[8].strftime("%d-%b-%Y (%H:%M:%S.%f)"))
    return G


def get_links(db:dbTools.dbConnection, G):

    sql = get_links_sql()
    db.cursor.execute(sql)
    mentions = db.cursor.fetchall()
    for mention in mentions:
        sql, val = search_tweet_sql(mention[3])
        db.cursor.execute(sql, val)
        tweet = db.cursor.fetchall()
        tweet = tweet[0]
        calc_weight = calc_edge_weight(db, {'id': mention[0], 'uid_to': mention[1],
                          'uid_from': mention[2], 'tweetid': mention[3],
                          'text': tweet[1]})
        G.add_edge(mention[2], mention[1],
                   texto=tweet[1], fecha=tweet[2].strftime("%d-%b-%Y (%H:%M:%S.%f)"),
                   generator=tweet[3], retweet_count=tweet[7],
                   favoritos=tweet[8], weight = calc_weight)
    return G


def calc_edge_weight(db:dbTools.dbConnection, mention):
    # check if this to and from pair have sent many messages
    sql = "SELECT * FROM twitter.mentions WHERE users_idfrom = %s " \
          "AND users_idto = %s"
    val = (mention['uid_from'], mention['uid_to'])
    db.cursor.execute(sql,val)
    try:
        # if it exists copy the id
        mention_results = db.cursor.fetchall()
        if len(mention_results) > 1:
            sql,val = build_tweet_compare_sql(mention_results)
            db.cursor.execute(sql, val)
            tweet_results = db.cursor.fetchall()
            flat_text = flatten_mentions(tweet_results)
            text_count = Counter(tweet_results)
            weight = flat_text.count(mention['text'])
        else:
            weight = db.cursor.rowcount

    except (mysql.connector.errors.InterfaceError, IndexError):
        weight = 1
    return weight


def flatten_mentions(tweets):
    tweet_text = []
    for tweet in tweets:
        tweet_text.append(tweet[0])
    return tweet_text


# ------SQL Getters-----
def build_tweet_compare_sql(mentions:list):
    sql ='SELECT alltext FROM twitter.tweet WHERE idtweet in ('
    val = tuple()
    count = 0
    for mention in mentions:
        count = count + 1
        if count == len(mentions):
            sql = sql + '%s'
        else:
            sql = sql + '%s, '
        val = val + (mention[3],)
    sql = sql + ')'
    return sql,val


def get_actors_sql():
    sql = "SELECT * FROM twitter.users"
    val = ()
    return sql, val


def get_links_sql():
    sql = "SELECT * FROM twitter.mentions"
    return sql


def search_tweet_sql(tweetid):
    sql = "SELECT * FROM twitter.tweet WHERE idtweet = %s"
    # Condition text and quote text
    val = (tweetid,)
    return sql, val


if __name__ == '__main__':
    create_graph('cNeg')
