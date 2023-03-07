--Query for Famous 10,000 Bitcoin Pizza Transaction
SELECT hash, block_timestamp, inputs.input_pubkey_base58
FROM `bigquery-public-data.crypto_bitcoin.transactions`
WHERE hash = 'a1075db55d416d3ca199f55b6084e2115b9345e16c5cf302fc80e9d5fbf5d48d';


--Compare Payments and Receipts Across Cryptocurrencies
-- SQL source from https://cloud.google.com/blog/products/data-analytics/introducing-six-new-cryptocurrencies-in-bigquery-public-datasets-and-how-to-analyze-them
SELECT
   address
,   type   
,   sum(value) as balance
FROM (
   -- debits
   SELECT
      array_to_string(inputs.addresses, ",") as address
   ,   inputs.type
   ,  -inputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.inputs` as inputs
   UNION ALL
   -- credits
   SELECT
      array_to_string(outputs.addresses, ",") as address
   ,   outputs.type
   ,   outputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.outputs` as outputs
) as double_entry_book
GROUP BY 1,2
ORDER BY balance DESC
LIMIT 100;


--Calculate the Gini of the Cryptocurrency Dash on Weekly Basis
WITH double_entry_book AS (
    -- debits
    SELECT
     addresses[offset(0)] as address
    , type
    , -value as value
    , block_timestamp
    FROM `bigquery-public-data.crypto_dash.inputs`
    CROSS JOIN UNNEST(addresses) as addresses
    UNION ALL
    -- credits
    SELECT
     addresses[offset(0)] as address
    , type
    , value as value
    , block_timestamp
    FROM `bigquery-public-data.crypto_dash.outputs`
    CROSS JOIN UNNEST(addresses) as addresses
), double_entry_book_by_date as (
    select
        date(block_timestamp) as date,
        address,
        sum(value / POWER(10,0)) as value
    from double_entry_book
    group by address, date
), daily_balances_with_gaps as (
    select
        address,
        date,
        sum(value) over (partition by address order by date) as balance,
        lead(date, 1, current_date()) over (partition by address order by date) as next_date
        from double_entry_book_by_date
), calendar as (
    select date from unnest(generate_date_array('2009-01-12', current_date())) as date
), daily_balances as (
    select address, calendar.date, balance
    from daily_balances_with_gaps
    join calendar on daily_balances_with_gaps.date <= calendar.date and calendar.date < daily_balances_with_gaps.next_date
), supply as (
    select
        date,
        sum(balance) as daily_supply
    from daily_balances
    group by date
), ranked_daily_balances as (
    select
        daily_balances.date,
        balance,
        row_number() over (partition by daily_balances.date order by balance desc) as rank
    from daily_balances
    join supply on daily_balances.date = supply.date
    where balance / daily_supply >= 0.0001
    ORDER BY balance / daily_supply DESC
)
select
    date,
    -- (1 − 2B) https://en.wikipedia.org/wiki/Gini_coefficient
    1 - 2 * sum((balance * (rank - 1) + balance / 2)) / count(*) / sum(balance) as gini
from ranked_daily_balances
group by date
order by date asc;


--Create a Table to Replace Previous Table & Store the transaction hash of the large mystery transfer of 194993 BTC in the table 51 inside the lab datase
CREATE OR REPLACE TABLE lab.51 (transaction_hash STRING) as
SELECT transaction_id 
FROM `bigquery-public-data.bitcoin_blockchain.transactions`
CROSS JOIN UNNEST(outputs) as outputs
WHERE outputs.output_satoshis = 19499300000000;


--Create Table to Replace the Previous Table & Store the balance of the pizza purchase address in the table 52 inside the lab dataset
CREATE OR REPLACE TABLE lab.52 (balance NUMERIC) as
WITH double_entry_book AS (
   -- debits
   SELECT
    inputs.addresses[offset(0)] as address
   , -inputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.inputs` as inputs
   UNION ALL
   -- credits
   SELECT
    outputs.addresses[offset(0)] as address
   , outputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.outputs` as outputs
)
SELECT
   sum(value) as balance
FROM double_entry_book
WHERE address = '1XPTgDRhN8RFnzniWCddobD9iKZatrvH4';

