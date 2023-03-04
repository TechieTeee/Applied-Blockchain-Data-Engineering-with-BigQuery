from google.cloud import bigquery
from igraph import Graph, plot
import pandas as pd

client = bigquery.Client()
project = 'bitcoin-bigquery'
laszlo = '1XPTgDRhN8RFnzniWCddobD9iKZatrvH4'

base_query_up = """SELECT
    timestamp/1000 AS tt,
    inputs.input_pubkey_base58 AS input_key,
    outputs.output_pubkey_base58 AS output_key,
    outputs.output_satoshis * 0.00000001 AS btc
    FROM
    `bigquery-public-data.bitcoin_blockchain.transactions`
    JOIN UNNEST (inputs) AS inputs
    JOIN UNNEST (outputs) AS outputs
    WHERE
    outputs.output_pubkey_base58 = '{address}'
    AND
    output_satoshis >= 10000000
    AND
    timestamp < TIMESTAMP_MILLIS({timestamp})
    AND
    inputs.input_pubkey_base58 IS NOT NULL
    GROUP BY tt, input_key, output_key, btc"""

frontier = pd.DataFrame()
tx0 = pd.DataFrame()

def dig(k, depth, tt, direction):
    global frontier, tx0
    base_query = base_query_up.format(address=k, timestamp=tt)
    if depth <= max_depth:
        print(depth, k)
        count_query = f"SELECT COUNT(*) FROM ({base_query})"
        query_job = client.query(count_query, project=project)
        if query_job.result().to_dataframe().iloc[0]['f0_'] > 0:
            query_job = client.query(base_query, project=project)
            frontier = query_job.result().to_dataframe()
            tx0 = pd.concat([tx0, frontier], ignore_index=True)

            for ikey in frontier['input_key'].unique():
                max_tt = frontier.loc[frontier['input_key'] == ikey, 'tt'].max()
                dig(ikey, depth+1, max_tt, direction)

max_depth = 2
dig(laszlo, 0, 999999999999999, -1)

btc = pd.DataFrame(tx0.groupby(['input_key', 'output_key'])['btc'].sum()).reset_index()
btc.net = Graph.DataFrame(btc, directed=True)

laszlo_idx = [i for i, v in enumerate(btc.net.vs['name']) if v == laszlo][0]
btc.net.vs['color'] = 'blue'
btc.net.vs['color'][laszlo_idx] = 'red'

mytitle = f'BTC inputs upstream of pizza purchase, depth={max_depth+2}'
btc.net.es['width'] = [int(v/800)+0.1 for v in btc.net.es['btc']]
plot(btc.net, 
     target=f"{mytitle}.png",
     vertex_size=5, 
     edge_arrow_size=0.5, 
     vertex_label=None,
     layout=btc.net.layout_nicely(),
     bbox=(800, 800),
     margin=50,
     main=mytitle)
