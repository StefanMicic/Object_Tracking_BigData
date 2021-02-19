import pandas as pd

# data = pd.read_csv("docker-streaming/producer/proba.csv")

# new_data = []
# for i, row in data.iterrows():
#     new_data.append(
#         [
#             row.timestamp,
#             row.person_id,
#             int(row.x_min + row.x_max) / 2,
#             int(row.y_min + row.y_max) / 2,
#             row.room_name,
#             "day-1",
#         ]
#     )

# df = pd.DataFrame(
#     new_data,
#     columns=["timestamp", "person_id", "center_x", "center_y", "room", "day"],
# )
# df.to_csv("data.csv", index=False)

data = [1, 1, 1, 1, 1]

for i, d in enumerate(data):
    print(i)
