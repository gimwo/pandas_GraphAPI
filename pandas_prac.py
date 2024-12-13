import pandas as pd
from dotenv import load_dotenv
import requests
import os

load_dotenv()

# The messages list to be converted to a dataframe and then to a csv
messages_list = []
"""
message_data['message'], message_data['from']['name'], message_data['from']['id'],
                                message_data['to']['data'][0]['name'], message_data['attachments']['data'] ,message_data['created_time']
"""

# function for creating a message item to be appended to list
def create_message_dict(data):
    message_item = {
        'message': data.get('message', ''),  # Safely get 'message', default to an empty string
        'from': data.get('from', {}).get('name', ''),  # Safely get 'name' from 'from'
        'from_psid': data.get('from', {}).get('id', ''),  # Safely get 'id' from 'from'
        'to': data.get('to', {}).get('data', [{}])[0].get('name', '') if data.get('to', {}).get('data') else '',
        # Safely get 'to'
        'created_time': data.get('created_time', ''),  # Safely get 'created_time'
        'attachments': data.get('attachments', {}).get('data', {})  # Safely get 'attachments', default to an empty dictionary
    }
    # print(message_item)
    messages_list.append(message_item)


# First we need to have access keys to the fb page
# access keys provide the necessary permissions
page_id= os.environ.get("PAGE_ID")
access_token = os.environ.get("FB_ACCESS_TOKEN")

conversations_url = f"https://graph.facebook.com/v21.0/{page_id}/conversations"
params = {
    "access_token": access_token
}

# Ok next we do the GraphAPI call
response = requests.get(conversations_url, params=params)
response_body = response.json()
conversations_data= response_body['data']

for thread in conversations_data:
    thread_id=thread['id']

    is_next_page = True

    while is_next_page:
        thread_data_url = f"https://graph.facebook.com/v21.0/{thread_id}/messages"
        thread_response = requests.get(thread_data_url, params=params)
        thread_data = thread_response.json()['data']
        # print(thread_response.json()['data'])

        # Get the messages
        for message in thread_data:

            message_id = message['id']
            message_data_url= f"https://graph.facebook.com/v21.0/{message_id}?fields=message,from,to,created_time"
            message_response = requests.get(message_data_url, params=params)

            print(message_response.json()['message'])
            message_data = message_response.json()
            create_message_dict(message_data)

        # pagination next
        after_key = thread_response.json()['paging']['cursors']['after']

        thread_after_url= f"https://graph.facebook.com/v21.0/{thread_id}/messages?after={after_key}"
        thread_after_response = requests.get(thread_after_url, params=params)

        # to check if no more messages on the next page if empty exit while loop
        if len(thread_after_response.json()['data']) == 0:
            is_next_page = False

print(messages_list)
df = pd.DataFrame(messages_list)

# data cleaning for messages with empty strings could be due to the message being only an attachment
df_cleaned = df[df['message'].str.strip() != ""]
df_cleaned.to_csv("messages.csv", index=False)
