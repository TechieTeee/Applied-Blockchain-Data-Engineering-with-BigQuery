
FROM
`bigquery-public-data.bitcoin_blockchain.transactions`
JOIN UNNEST (inputs) AS inputs
JOIN UNNEST (outputs) AS outputs
WHERE
outputs.output_pubkey_base58 = 'ADDRESS'
AND
output_satoshis  >= 10000000
AND
timestamp < TIMESTAMP * 1000
AND
inputs.input_pubkey_base58 IS NOT NULL
GROUP BY tt, input_key, output_key, btc
