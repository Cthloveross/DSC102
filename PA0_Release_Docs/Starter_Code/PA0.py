import json
import ctypes
from dask.distributed import Client
import dask.dataframe as dd

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)

def PA0(path_to_user_reviews_csv):
    client = Client()
    # Helps fix any memory leaks.
    client.run(trim_memory)
	client = client.restart()
    user_reviews_ddf = dd.read_csv("user_reviews.csv")
    user_reviews_ddf['number_products_rated'] = user_reviews_ddf['asin']
    user_reviews_ddf['avg_ratings'] = user_reviews_ddf['overall']
    # we want to xtract the year
    user_reviews_ddf['reviewing_since'] = user_reviews_ddf['reviewTime'].str.slice(-4).astype("int")
    #  extract the number of helpful votes from the 'helpful' column
    user_reviews_ddf['helpful_votes'] = user_reviews_ddf['helpful'].apply(lambda x: eval(x)[0], meta=('x', 'int'))
    #  extract the number of total votes from the 'helpful' column
    user_reviews_ddf['total_votes'] = user_reviews_ddf['helpful'].apply(lambda x: eval(x)[1], meta=('x', 'int'))
    
    result_df = user_reviews_ddf.groupby("reviewerID", sort=False).agg({"number_products_rated":"count", "avg_ratings":"mean", "reviewing_since":"min", "helpful_votes":"sum", "total_votes":"sum"}, split_out = 8)
    


    
    
    submit = <YOUR_USERS_DATAFRAME>.describe().compute().round(2)    
    with open('results_PA0.json', 'w') as outfile: 
        json.dump(json.loads(submit.to_json()), outfile)