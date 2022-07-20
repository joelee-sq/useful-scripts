# payments-raw-production project in bigquery

# with raw as (
#   select
#     payment_id,
#     card_transaction.merchant.locale.country_code,
#     card_transaction.auth_intent.total_amount.amount,
#     timestamp_add(
#       TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(card_transaction.auth_intent.created_at_ms), HOUR),
#       interval (div(EXTRACT(MINUTE from TIMESTAMP_MILLIS(card_transaction.auth_intent.created_at_ms)), 5)*5) MINUTE
#     ) as created_at_bucketed,
#     feed_sync_id,
#     max(feed_sync_id) over (partition by payment_id) as max_feed_sync_id
#   from `payments-raw-production.feeds.esperanto_raw_records_v2`
#   where _PARTITIONTIME between '2022-07-15' and  '2022-07-16'
#     and TIMESTAMP_MILLIS(card_transaction.auth_intent.created_at_ms)
#       between timestamp('2022-07-15 17:00:00+00') and timestamp('2022-07-15 20:59:59+00')
# )
# select 
#   country_code,
#   created_at_bucketed,
#   count(*) as count_payments,
#   sum(amount) as sum_auth_intent_amount
# from raw
# where feed_sync_id = max_feed_sync_id
# group by created_at_bucketed, country_code
# order by country_code, created_at_bucketed;

# Save Results => Save as CSV

import csv
import numpy as np
from dateutil import parser
from collections import defaultdict

TIME_RANGE = [
    '2022-07-15 17:00:00+00',
    '2022-07-15 20:59:59+00'
]

SEV_RANGE = [
    '2022-07-15 18:30:00+00',
    '2022-07-15 19:30:00+00'
]

SEV_RANGE_TIMESTAMPS = [
    parser.parse(SEV_RANGE[0]).timestamp(), 
    parser.parse(SEV_RANGE[1]).timestamp()
]

def quad_fit(x, y):
    return np.poly1d(np.polyfit(x, y, 2))

def parse_data(data):
    return [[
      row[0],
      parser.parse(row[1]).timestamp(),
      int(row[2]),
      int(row[3])
    ] for row in data]

def process(data, countries = {}):
    country_data = [row for row in data if len(countries) == 0 or row[0] in countries]
    not_sev = [row for row in country_data if SEV_RANGE_TIMESTAMPS[0] > row[1] or row[1] > SEV_RANGE_TIMESTAMPS[1]]

    not_sev_t = [row[1] for row in not_sev]
    not_sev_count = [row[2] for row in not_sev]
    not_sev_amount = [row[3] for row in not_sev]

    model_count = quad_fit(not_sev_t, not_sev_count)
    model_amount = quad_fit(not_sev_t, not_sev_amount)

    total_diff_count = 0
    total_diff_amount = 0

    for row in country_data:
        if SEV_RANGE_TIMESTAMPS[0] > row[1] or row[1] > SEV_RANGE_TIMESTAMPS[1]:
            continue
        t = row[1]
        sev_count_at_t = row[2]
        sev_amount_at_t = row[3]
        expected_count = int(model_count(t))
        expected_amount = int(model_amount(t))
        diff_count = expected_count - sev_count_at_t
        diff_amount = expected_amount - sev_amount_at_t
        # print(t, diff_count, diff_amount)
        total_diff_count += diff_count
        total_diff_amount += diff_amount
    print(f"{str(countries):6} ## Count Diff: {total_diff_count:8,}, Amount Diff: {total_diff_amount / 100:15,}")


with open('bquxjob_319a0a35_1821370d7b6.csv') as csvfile:
    reader = csv.reader(csvfile)
    data = parse_data(list(reader)[1:])
    countries = {row[0] for row in data}

    print("")
    print("Estimated loss in payments as compared to a best-fit regression")
    print("Data fetched from BigQuery")
    print()
    print(f"Data from {TIME_RANGE[0]} to {TIME_RANGE[1]}")
    print(f" SEV from {SEV_RANGE[0]} to {SEV_RANGE[1]}")
    print()

    for country in ["US", "CA", "GB", "JP", "IE", "ES", "FR", "AU"]:
        process(data, {country})

    overall = defaultdict(lambda: [0, 0])
    for row in data:
        overall[row[1]][0] += row[2]
        overall[row[1]][1] += row[3]
    total_data = [["", k, v[0], v[1]] for k, v in overall.items()]
    print("")
    print("Total:")
    process(total_data, {})
