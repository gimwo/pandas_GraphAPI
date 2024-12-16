import pandas as pd
from dotenv import load_dotenv
import requests
import os

load_dotenv(dotenv_path="./.env-oth")

# The messages list to be converted to a dataframe and then to a csv
messages_list = []
# First we need to have access keys to the fb page
# access keys provide the necessary permissions
page_id= os.environ.get("PAGE_ID")
access_token = os.environ.get("FB_ACCESS_TOKEN")


# function for creating a message item to be appended to list
def create_message_dict(data):

    message_item = {
        'message': data.get('message', ''),
        'from': data.get('from', {}).get('name', ''),
        'from_psid': data.get('from', {}).get('id', ''),
        'to': data.get('to', {}).get('data', [{}])[0].get('name', '') if data.get('to', {}).get('data') else '',
        'created_time': data.get('created_time', ''),
        'attachments': data.get('attachments', {})
    }
    # print(message_item)
    messages_list.append(message_item)

def prepare_dataframe(msg_list):
    rows = []
    # print(f"list: {msg_list}")
    for message_obj in msg_list:
        # print(type(message_obj),message_obj)
        if 'attachments' in message_obj and 'data' in message_obj['attachments']:
            for attachment in message_obj['attachments']['data']:
                print(attachment)
                rows.append({
                    'message': message_obj['message'],
                    'from': message_obj['from'],
                    'from_psid': message_obj['from_psid'],
                    'to': message_obj['to'],
                    'created_time': message_obj['created_time'],
                    'attachment_id': attachment.get('id', ''),
                    'attachment_mime_type': attachment.get('mime_type', ''),
                    'attachment_name': attachment.get('name', ''),
                    'attachment_size': attachment.get('size', ''),
                })
        else:
            # Add a row for messages without attachments
            rows.append({
                'message': message_obj['message'],
                'from': message_obj['from'],
                'from_psid': message_obj['from_psid'],
                'to': message_obj['to'],
                'created_time': message_obj['created_time'],
                'attachment_id': '',
                'attachment_mime_type': '',
                'attachment_name': '',
                'attachment_size': '',
            })

    return rows



conversations_url = f"https://graph.facebook.com/v21.0/{page_id}/conversations"
params = {
    "access_token": access_token
}

# Ok next we do the GraphAPI call
response = requests.get(conversations_url, params=params)
response_body = response.json()
conversations_data= response_body['data']

print(conversations_data)

for thread in conversations_data:
    thread_id=thread['id']
    print(thread_id)

    is_next_page = True

    thread_data_url = f"https://graph.facebook.com/v21.0/{thread_id}/messages"
    while is_next_page:
        thread_response = requests.get(thread_data_url, params=params)
        thread_data = thread_response.json()['data']
        # print(thread_response.json()['data'])

        # Get the messages
        for message in thread_data:

            message_id = message['id']
            print(message_id)
            message_data_url= f"https://graph.facebook.com/v21.0/{message_id}?fields=message,from,to,created_time,attachments"
            message_response = requests.get(message_data_url, params=params)

            print(message_response.json()['message'])
            message_data = message_response.json()
            create_message_dict(message_data)


        # pagination next
        thread_data_url = thread_response.json()['paging'].get('next', '')
        print(thread_data_url)

        # to check if no more messages on the next page if empty exit while loop
        if thread_data_url == '':
            is_next_page = False

# print(messages_list)
# print(f"LIST: {messages_list}")
df_rows = prepare_dataframe(messages_list)
df = pd.DataFrame(df_rows)

# data cleaning for messages with empty strings could be due to the message being only an attachment
df_cleaned = df[(df['message'].fillna("").str.strip() != "") | (df['attachment_id'].fillna("").str.strip() != "")]
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 10)
print(df_cleaned)
df_cleaned.to_csv("messages.csv", index=False)
