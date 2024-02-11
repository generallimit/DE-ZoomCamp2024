# Convert UNIX timestamp; Python


import datetime
timestamp_with_ms = 1626325266384
dt = datetime.datetime.fromtimestamp(timestamp_with_ms / 1000)
formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
print(formatted_time)

# 2021-07-15 07:01:06.384

# 1640996061000000000

