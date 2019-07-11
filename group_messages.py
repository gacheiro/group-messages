import json
from itertools import groupby


JSON = 'data.json'
TIME_WINDOW = 120 # 2 minutes


def load(file):
    with open(file) as f:
        data = json.load(f)
    return data


def groupby_users(data):
    users = []
    groups = [] 

    data = sorted(data, key=lambda u: u['user'])
    for user, msgs in groupby(data, key=lambda u: u['user']):
        groups.append(list(msgs))
        users.append(user)
    return users, groups


def groupby_ts(messages, time_window=TIME_WINDOW):
    """Group messages by timestamps. All messages within the 
    time_window (in sec) shoud be grouped together.
    
    Returns a dict with the timestamp as key and grouped msgs as value."""

    sorted_msgs = sorted(messages, key=lambda m: float(m['ts']))

    # TODO: Maybe use 'takewhile' to group the messages
    # start the group with the first msg
    first_msg = sorted_msgs[0]
    key = first_msg['ts']
    grouped_msgs = {
        key: [sorted_msgs[0]],
    }
    for msg in sorted_msgs[1:]:
        if float(msg['ts']) - float(key) <= time_window:
            grouped_msgs[key].append(msg)
        else:
            # start a new group
            key = msg['ts']
            grouped_msgs[key] = [msg]

    return grouped_msgs


def main():
    data = load(JSON)
    users, messages = groupby_users(data)

    for user, msgs in zip(users, messages):
        print(f'User {user} has {len(msgs)} messages')
        grouped_msgs = groupby_ts(msgs)

        # do some tests
        for ts, gmsgs in grouped_msgs.items():
            for msg in gmsgs:
                assert float(ts) - float(msg['ts']) <= TIME_WINDOW

        with open(f'output/{user}.json', 'w') as f:
            json.dump(grouped_msgs, f, indent=4)


if __name__ == '__main__':
    main()
