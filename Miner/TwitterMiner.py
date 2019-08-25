from searchtweets import ResultStream, gen_rule_payload, load_credentials, collect_results
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import twitter
import time

# Collect data
def coll_cantera_neg():
    premium_search_args = load_credentials(filename="./twitter_keys.yaml",
                                           yaml_key="search_tweets_30_day_dev",
                                           env_overwrite=False)
    bcp = search_lima('(bcp OR BCPComunica)', premium_search_args)
    bbva = search_lima('(bbva)', premium_search_args)
    interbank = search_lima('(interbank)', premium_search_args)
    yape = search_lima('(yape)', premium_search_args)
    scotia = search_lima('(scotiabank)', premium_search_args)
    all_search = bcp + bbva + interbank + yape + scotia
    return all_search


def coll_cantera_pos():
    premium_search_args = load_credentials(filename="./twitter_keys.yaml",
                                           yaml_key="search_tweets_30_day_dev",
                                           env_overwrite=False)


def coll_user_info(name):
    # api = twitter.Api(consumer_key='Wa5xi8yfBZ4LihhpZp2KqzlOq',
    #                   consumer_secret='JHZn4GSi08P6e2S71eRAOT2cDWBk0VrYbMwOg0XhzssOALbsDE',
    #                   access_token_key='86863810-NA4wtMzKrQ62EMIvFUyIaTlXuIWGjd5QwlZkJBL4P',
    #                   access_token_secret='DuhCa5Kg3zjHJykC3W30wPxteEwz5QGEQZvoDAqiVwM5o')
    api = twitter.Api(consumer_key='C0Q2slgd38EQUV82soOig68Uo',
                      consumer_secret='JKJ0tVC8vnlDmVbvPT4BF67nx7r5VqnJTSPHMiGqJLo43bba3m',
                      access_token_key='479643521-Q7wHBkOomWOSa7j2jqiKrh5i8VSCbnZewOy0lUJv',
                      access_token_secret='HSdLbWQiLXtwpZKKI3W2iW98oDk3QJbrGBEGYmAHhlwU4')
    return api.GetUser(screen_name=name)


# Twitter Control
def search_lima(search, premium_search_args):
    # rule = gen_rule_payload(search + " point_radius:[-12.089282 -77.020041 10mi]", results_per_call=100)
    rule = gen_rule_payload(search + "place:Peru", results_per_call=100)
    data = collect_results(rule,
                           max_results=100,
                           result_stream_args=premium_search_args)
    return data


def search_user(search, premium_search_args):
    rule = gen_rule_payload('bio_name:' + search, results_per_call=5)
    data = collect_results(rule,
                           max_results=5,
                           result_stream_args=premium_search_args)
    return data


# SQl Outer function
def save_to_sql(raw_mat, db):
    global conn
    tweet_id = None
    user_ment_bool = False
    """ Connect to MySQL database """
    hostname = '127.0.0.1'
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
            mycursor = conn.cursor()

        for tweet in raw_mat:
            # -------1)save tweet information-------
            # check if tweet exists
            sql, val = find_tweet(tweet)
            mycursor.execute(sql, val)
            try:
                # if exists get id
                tweet_id = mycursor.fetchall()
                tweet_id = tweet_id[0][0]
            except (mysql.connector.errors.InterfaceError, IndexError):
                # if doesn't exist create and get id
                sql, val = get_insert_tweet_vals(tweet)
                mycursor.execute(sql, val)
                conn.commit()
                tweet_id = mycursor.lastrowid
            # ------2)save source actor info-------

            # Does actor exist in db?
            sql, val = find_actor(tweet['user'])
            mycursor.execute(sql, val)
            try:
                # if exists then fetch id and update data if it's missing
                myresult = mycursor.fetchall()
                from_id = myresult[0][0]
                sql, val = tst_actor_complete_sql(from_id)
                mycursor.execute(sql, val)
                tst_created_at = mycursor.fetchall()
                # Do I need to update?
                if tst_created_at[0][0] is None:
                    sql, val = refill_actor_sql(tweet['user'], from_id)
                    mycursor.execute(sql, val)
                    conn.commit()
            except (mysql.connector.errors.InterfaceError, IndexError):
                # if it doesn't exist create it and set id to None
                sql, val = get_insert_actor_vals(tweet['user'])
                mycursor.execute(sql, val)
                conn.commit()
                from_id = mycursor.lastrowid
            # ---------3)User Mentions-------------
            if tweet.user_mentions:
                for user in tweet.user_mentions:
                    # check if user exists
                    sql, val = find_actor(user)
                    mycursor.execute(sql, val)
                    try:
                        # if it exists copy the id
                        myresult = mycursor.fetchall()
                        to_id = myresult[0][0]
                    except (mysql.connector.errors.InterfaceError, IndexError):
                        # if it doesn't exist create it and copy the id
                        myresult = []
                        # ---- Account is suspended in other api-----
                        try:
                            time.sleep(0.5)
                            user_data = coll_user_info(user['screen_name'])
                            sql, val = get_insert_um_actor_vals(user_data)
                        except:
                            sql, val = get_insert_sm_actor_vals(user)
                        # -------------------------------------------------
                        #sql, val = get_insert_sm_actor_vals(user)
                        mycursor.execute(sql, val)
                        conn.commit()
                        to_id = mycursor.lastrowid
                    # find the sources id
                    sql, val = find_actor(tweet['user'])
                    mycursor.execute(sql, val)
                    myresult = mycursor.fetchall()
                    from_id = myresult[0][0]

                    # ----save to mentions table-----
                    # Check if mention exists
                    sql, val = find_mention_sql(to_id, from_id, tweet_id)
                    mycursor.execute(sql, val)
                    try:
                        myresult = mycursor.fetchall()
                        mention_id = myresult[0][0]
                    except (mysql.connector.errors.InterfaceError, IndexError):
                        sql, val = get_insert_mentions_vals(to_id, from_id, tweet_id)
                        mycursor.execute(sql, val)
                        conn.commit()
                    if user['screen_name'] is tweet.in_reply_to_screen_name \
                            and tweet.in_reply_to_screen_name is not None:
                        user_ment_bool = False
                    else:
                        user_ment_bool = True
                    # save user mention actor info
            if user_ment_bool:
                pass
            # for user in tweet.in_reply_to_screen_name:
            #     sql, val = find_actor(user)
            #     mycursor.execute(sql, val)
            #     try:
            #         myresult = mycursor.fetchall()
            #         to_id = myresult[0][0]
            #     except mysql.connector.errors.InterfaceError:
            #         myresult = []
            #         sql, val = get_insert_sm_actor_vals(user)
            #         mycursor.execute(sql, val)
            #         conn.commit()
            #         sql, val = find_actor(user)
            #         mycursor.execute(sql, val)
            #         myresult = mycursor.fetchall()
            #         to_id = myresult[0][0]
            #     sql, val = find_actor(tweet['user'])
            #     mycursor.execute(sql, val)
            #     myresult = mycursor.fetchall()
            #     from_id = myresult[0][0]

            print(mycursor.rowcount, "record inserted.")
        # search for screen name

    except Error as e:
        print(e)

    finally:
        conn.close()


# UPDATES
def refill_actor_sql(full_actor_info, targetid):
    tmp_desc = None
    created_dt = None
    sql = "UPDATE twitter.users SET " \
          "description = %s, followers_count = %s," \
          "friends_count = %s,listed_count = %s," \
          "favorites_count = %s,created_at = %s " \
          "WHERE idusers = %s"
    if full_actor_info['description'] is not None:
        tmp_desc = full_actor_info['description']
        if len(tmp_desc) > 200:
            tmp_desc = tmp_desc[:200]
    else:
        t=1

    if full_actor_info['created_at'] is not None:
        created_dt = datetime.strptime(full_actor_info['created_at'], '%a %b %d %H:%M:%S %z %Y')

    val = (tmp_desc, full_actor_info['followers_count'],
           full_actor_info['friends_count'], full_actor_info['listed_count'],
           full_actor_info['favourites_count'], created_dt,
           targetid)
    return sql, val


# SEARCHES
def find_mention_sql(to_id, from_id, tweet_id):
    sql = "SELECT idmentions FROM twitter.mentions WHERE users_idto = %s " \
          "AND users_idfrom = %s AND tweet_idtweet = %s"
    val = (to_id, from_id, tweet_id)
    return sql, val


def tst_actor_complete_sql(targetid):
    sql = "SELECT created_at FROM twitter.users WHERE idusers = %s"
    val = (targetid,)
    # val = ()
    return sql, val


def find_actor(actor_info):
    sql = "SELECT idusers FROM twitter.users WHERE screen_name = %s"
    val = (actor_info['screen_name'],)
    return sql, val


def find_tweet(tweet):
    sql = "SELECT idtweet FROM twitter.tweet WHERE created_at_datetime = %s " \
          "AND alltext = %s AND generator = %s AND quote_or_rt_text =%s"
    # Condition text and quote text
    tmp_txt = tweet.all_text
    tmp_quote = tweet.quote_or_rt_text

    if len(tmp_txt) > 200:
        tmp_txt = tmp_txt[:200]
    if len(tmp_quote) > 200:
        tmp_quote = tweet.quote_or_rt_text[:200]
    val = (tweet.created_at_datetime, tmp_txt,
           tweet.generator['name'], tmp_quote)
    return sql, val


# INSERTS
def get_insert_mentions_vals(to_id, from_id, tweet_id):
    sql = "INSERT INTO twitter.mentions (users_idto, users_idfrom, tweet_idtweet)" \
          "VALUES (%s, %s, %s)"
    val = (to_id, from_id, tweet_id)
    return sql, val


def get_insert_sm_actor_vals(actor_info):
    # inputs actor info when read from user mentions
    sql = "INSERT INTO twitter.users (screen_name, name," \
          "description, followers_count," \
          "friends_count,listed_count," \
          "favorites_count,created_at) " \
          "VALUES (%s, %s," \
          "%s, %s," \
          "%s, %s," \
          "%s, %s)"
    val = (actor_info['screen_name'], actor_info['name'],
           '', None,
           None, None,
           None, None)

    return sql, val


def get_insert_um_actor_vals(user):
    # user = tweet['user']
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


def get_insert_actor_vals(user):
    # user = tweet['user']
    sql = "INSERT INTO twitter.users (screen_name, name," \
          "description, followers_count," \
          "friends_count,listed_count," \
          "favorites_count,created_at) " \
          "VALUES (%s, %s," \
          "%s, %s," \
          "%s, %s," \
          "%s, %s)"
    tmp_desc = user['description']
    if tmp_desc is not None:
        if len(tmp_desc) > 200:
            tmp_desc = tmp_desc[:200]
    else:
        tmp_desc = None
    if user['created_at'] is not None:
        created_dt = datetime.strptime(user['created_at'], '%a %b %d %H:%M:%S %z %Y')
    else:
        created_dt = None
    val = (user['screen_name'], user['name'],
           tmp_desc, user['followers_count'],
           user['friends_count'], user['listed_count'],
           user['favourites_count'], created_dt)

    return sql, val


def get_insert_tweet_vals(tweet):
    sql = "INSERT INTO twitter.tweet (alltext, created_at_datetime," \
          "generator, quote_or_rt_text," \
          "quote_count," \
          "retweet_count,favorite_count) " \
          "VALUES (%s, %s," \
          "%s, %s," \
          "%s," \
          "%s, %s)"
    tmp_txt = tweet.all_text
    tmp_quote = tweet.quote_or_rt_text

    if len(tmp_txt) > 200:
        tmp_txt = tmp_txt[:200]
    if len(tmp_quote) > 200:
        tmp_quote = tweet.quote_or_rt_text[:200]

    val = (tmp_txt, tweet.created_at_datetime,
           tweet.generator['name'], tmp_quote,
           tweet.quote_count,
           tweet.retweet_count, tweet.favorite_count)
    return sql, val


if __name__ == '__main__':
    #coll_user_info('ClaroPeru')
    raw_neg = coll_cantera_neg()
    save_to_sql(raw_neg, 'canteraneg')
    print('Cantera Negativa Minada!')
