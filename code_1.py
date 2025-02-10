import requests
import base64

# Configuration. DO NOT COMMIT
CLIENT_ID = ''
CLIENT_SECRET = ''
USERNAME = ''
PASSWORD = ''
API_VERSION = '50.0'

# OAuth endpoint URL for production (replace with sandbox if needed)
OAUTH_URL = 'https://playful-impala-unw1qi-dev-ed.trailblaze.my.salesforce.com/services/oauth2/token'


def get_access_token():
    """Get access token using client credentials."""
    auth = {
        'grant_type': 'password',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'username': USERNAME,
        'password': PASSWORD
    }
    response = requests.post(OAUTH_URL, data=auth)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Failed to get access token: {response.text}")


def fetch_attachments(instance_url, access_token):
    """Fetch all attachments using SOQL query."""
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # Query attachments with a limit (adjust as needed). Apply filters to get correct records
    soql_query = (
        "SELECT Id, Name, Description, ParentId, ContentType FROM Attachment" # WHERE CreatedDate >= LAST_N_DAYS:7"
    )
    url = f'{instance_url}/services/data/v{API_VERSION}/query'

    response = requests.get(url, headers=headers, params={'q': soql_query})
    if response.status_code == 200:
        result = response.json()
        #print(f'fetch attachment response: {result}')
        return result.get('records', [])
    else:
        raise ValueError(f"SOQL query failed: {response.text}")
    
def save_attachment_file(filename, content):
    """Save the content of an attachment to local disk."""
    try:
        with open(filename, 'wb') as f:
                f.write(content)
    except Exception as e:
        print(f'An error occured while saving file: {str(e)}')


def get_attachment_content(instance_url, access_token, attachment):
    """Get the file content from the attachments"""
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    file_id = attachment.get('Id')
    if not file_id:
        raise ValueError("Attachment Id is missing.")

    # URL to get the file content
    url = f'{instance_url}/services/data/v{API_VERSION}/sobjects/Attachment/{file_id}/Body'

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        filename = attachment.get('Name', 'file_') #+ '.content'
        #save_attachment_file(filename, response.content) uncomment this if you want to save the attachments locally
        # alternatively you can call save_attachment_file() from anywhere 
        return filename, response.content
    else:
        raise ValueError(f"Failed to retrieve file content for {file_id}: {response.text}")


def create_attachment(instance_url, access_token, attachment, file_content):
    """Used to create attachments in target org"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': f'application/json'
    }

    url = f'{instance_url}/services/data/v{API_VERSION}/sobjects/Attachment/'

    payload = {
        'Name': 'test' + attachment.get('Name',''),
        'Description': attachment.get('Descriptionn',''),
        'Body': base64.b64encode(file_content).decode("utf-8"), # base64 encoding is necessary. if you dont decode it to utf-8, it remains in binary format which isnt supported
        'ParentId': attachment.get('ParentId'),
        'ContentType': attachment.get('ContentType')
    }

    try:
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 201:
            print('Successfully created attachment record')
            print(f'response: {response.json()}')
            return response.json()['id']
        else:
            print(f'Couldnt create attachment record. Error: {response.json()}')

    except Exception as e:
        print(f'An error occured: ', str(e))

# Redundant function. I thought I would attach files separately once the attachment records are created
# but I can create attachments along with the file if I encode the file content in base64 format
def attach_file(instance_url, access_token, attachmentId, file_content, content_type):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json' #content_type
    }
    url = f'{instance_url}/services/data/v{API_VERSION}/sobjects/Attachment/{attachmentId}/body'

    encoded_file = base64.b64encode(file_content).decode("utf-8")

    try:
        response = requests.post(url, data=encoded_file, headers=headers)

        if response == 201:
            print(f'File attached successfully to {attachmentId}')
        else:
            print(f'An error occured while attaching a file')
            print(f'Error: {response.json()}')
    except Exception as e:
        print(f'An error occured while attaching a file: {response.json()}')


def main():
    """Main function"""
    try:
        auth_response = get_access_token()
        access_token = auth_response.get('access_token')
        instance_url = auth_response.get('instance_url')

        #print(auth_response)

        if not access_token or not instance_url:
            raise ValueError("Invalid OAuth response: missing token or instance URL.")

        attachments = fetch_attachments(instance_url, access_token)
        print(f"Retrieved {len(attachments)} attachments.")
        print(f'attachments info: {attachments}')

        # Save each attachment's content
        for idx, att in enumerate(attachments):
            filename, file_content = get_attachment_content(instance_url, access_token, att)
            #print(f"Saved file: {att.get('Name')} [{idx + 1}/{len(attachments)}]")

            #new_attachment_id = create_attachment(instance_url, access_token, att, file_content)
            #print(f'created attachment: {new_attachment_id}')
            #filename = 'test' + filename
            #attach_file(instance_url, access_token, new_attachment_id, file_content, att.get('ContentType'))
            #print(f'attached: {filename} to record: {new_attachment_id}')
            #break

            


    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()