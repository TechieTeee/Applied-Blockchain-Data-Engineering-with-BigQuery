--Select the Famous Pizza Bitcoin Transaction
SELECT
  timestamp/1000 AS tt,
  inputs.input_pubkey_base58 AS input_key,
  outputs.output_pubkey_base58 AS output_key,
  outputs.output_satoshis * 0.00000001 AS btc
FROM
  `bigquery-public-data.bitcoin_blockchain.transactions_partitioned` -- partitioned table
JOIN UNNEST(inputs) AS inputs
JOIN UNNEST(outputs) AS outputs
ON outputs.transaction_hash = transactions.hash
WHERE
  outputs.output_pubkey_base58 = 'ADDRESS'
  AND output_satoshis >= 10000000
  AND timestamp < TIMESTAMP * 1000
  AND inputs.input_pubkey_base58 IS NOT NULL
  AND timestamp >= '2022-01-01' -- relevant partition start time
  AND timestamp < '2022-02-01' -- relevant partition end time
GROUP BY tt, input_key, output_key, btc;

