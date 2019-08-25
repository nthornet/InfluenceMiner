import twitter
from searchtweets import ResultStream, gen_rule_payload, load_credentials, collect_results
import json
import os.path

user_list = []
followers_list = []

# api = twitter.Api(consumer_key='C0Q2slgd38EQUV82soOig68Uo',
#                   consumer_secret='JKJ0tVC8vnlDmVbvPT4BF67nx7r5VqnJTSPHMiGqJLo43bba3m',
#                   access_token_key='479643521-Q7wHBkOomWOSa7j2jqiKrh5i8VSCbnZewOy0lUJv',
#                   access_token_secret='HSdLbWQiLXtwpZKKI3W2iW98oDk3QJbrGBEGYmAHhlwU4')

# api = twitter.Api(consumer_key='Wa5xi8yfBZ4LihhpZp2KqzlOq',
#                   consumer_secret='JHZn4GSi08P6e2S71eRAOT2cDWBk0VrYbMwOg0XhzssOALbsDE',
#                   access_token_key='86863810-NA4wtMzKrQ62EMIvFUyIaTlXuIWGjd5QwlZkJBL4P',
#                   access_token_secret='DuhCa5Kg3zjHJykC3W30wPxteEwz5QGEQZvoDAqiVwM5o')

premium_search_args  = load_credentials(filename="./twitter_keys.yaml",
                 yaml_key="search_tweets_30_day_dev",
                 env_overwrite=False)
rule = gen_rule_payload("bcp point_radius:[-77.0304221 -12.1217806 20mi]", results_per_call=100)
bcp = collect_results(rule,
                max_results=100,
                result_stream_args=premium_search_args)
[print(tweet.all_text) for tweet in results[0:10]]


# %%
# Load File
if os.path.isfile('Miner/bcp.json'):
    with open('Miner/bcp.json') as json_file:
        past_res_json = json.load(json_file)
    past_res = [twitter.Status.NewFromJsonDict(x) for x in past_res_json.get('statuses', '')]
else:
    past_res = None

# results = api.GetSearch(raw_query="q=banco bcp", geocode='-12.04902,-77.03360,10km', return_json=True)
# with open('bcp.json', 'w') as f:
#     json.dump(results, f)
# %%
# Get credentials and search
rawurl = 'https://api.twitter.com/1.1/search/tweets.json?q=from%3Atwitterdev&result_type=mixed&count=2'
results_json = api.GetSearch(term='bcp')
results = [twitter.Status.NewFromJsonDict(x) for x in results_json.get('statuses', '')]
# %%
if past_res:
    total_result = past_res+results
# %%
with open('bcp.json', 'w') as f:
    json.dump(total_result, f)
results = [twitter.Status.NewFromJsonDict(x) for x in results.get('statuses', '')]
# %%

for tweet in results:
    tmptweet = tweet.AsDict()
    # print(tmptweet)
    user_list.append(tmptweet['user']['id'])
    print(tmptweet['user']['screen_name'])
# %%
for tweet in results:
    tmptweet = tweet.AsDict()
