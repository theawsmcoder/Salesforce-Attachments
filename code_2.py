import base64
import requests
 
class SalesforceOrg:
    """Connect with the salesforce org with the credentials using this class"""
 
    API_VERSION = '50.0'
 
    def __init__(self, client_id, client_secret, username, password, auth_url):
        self.grant = 'password'
        self.username = username
        self._client_id = client_id
        self._client_secret = client_secret
        self.password = password
        self.auth_url = auth_url.split('.com/')[0] + '.com/services/oauth2/token'
 
 
    def authenticate(self):
        """Get access token using client credentials."""
 
        auth = {
            'grant_type': 'password',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'username': self.username,
            'password': self.password
        }
 
        response = requests.post(self.auth_url, data=auth)
 
        if response.status_code == 200:
            auth_response = response.json()
            self.instance_url = auth_response.get('instance_url')
            self.access_token = auth_response.get('access_token')
            return auth_response
        else:
            raise ValueError(f"Failed to get access token: {response.text}")
       
 
    def fetch_attachments(self, query):
        """Fetch all attachments using SOQL query."""
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
   
        # Query attachments with a limit (adjust as needed). Apply filters to get correct records
        soql_query = (
            query #"SELECT Id, Name, Description, ParentId, ContentType FROM Attachment" # WHERE CreatedDate >= LAST_N_DAYS:7"
        )
        url = f'{self.instance_url}/services/data/v{self.API_VERSION}/query'
   
        response = requests.get(url, headers=headers, params={'q': soql_query})
        if response.status_code == 200:
            result = response.json()
            #print(f'fetch attachment response: {result}')
            return result.get('records', [])
        else:
            raise ValueError(f"SOQL query failed: {response.text}")
       
   
    def get_attachment_content(self, attachment):
        """Get the file content from the attachments"""
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
   
        file_id = attachment.get('Id')
        if not file_id:
            raise ValueError("Attachment Id is missing.")
   
        # URL to get the file content
        url = f'{self.instance_url}/services/data/v{self.API_VERSION}/sobjects/Attachment/{file_id}/Body'
   
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            filename = attachment.get('Name', 'file_') #+ '.content'
            #save_attachment_file(filename, response.content) #uncomment this if you want to save the attachments locally
            # alternatively you can call save_attachment_file() from anywhere
            return filename, response.content
        else:
            raise ValueError(f"Failed to retrieve file content for {file_id}: {response.text}")
       
   
    def save_attachment_file(self, filename, content):
        """Save the content of an attachment to local disk."""
        try:
            with open(filename, 'wb') as f:
                    f.write(content)
        except Exception as e:
            print(f'An error occured while saving file: {str(e)}')
 
 
    def create_attachment(self, attachment, file_content):
        """Used to create attachments in target org"""
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': f'application/json'
        }
   
        url = f'{self.instance_url}/services/data/v{self.API_VERSION}/sobjects/Attachment/'
   
        payload = {
            'Name': attachment.get('Name',''),
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
 
 
    def fetch_parent_record(self, query):
        """Fetch the parent records using SOQL query."""
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
   
        # Query attachments with a limit (adjust as needed). Apply filters to get correct records
        soql_query = (
            query
        )
        url = f'{self.instance_url}/services/data/v{self.API_VERSION}/query'
   
        response = requests.get(url, headers=headers, params={'q': soql_query})
        if response.status_code == 200:
            result = response.json()
            #print(f'fetch attachment response: {result}')
            return result.get('records', [])
        else:
            raise ValueError(f"SOQL query failed: {response.text}")
 
 
    def create_parent_record(self, parent_record):
        """Used to create parent records in target org"""
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': f'application/json'
        }
 
        object_name = parent_record.get('attributes')['type']
   
        url = f'{self.instance_url}/services/data/v{self.API_VERSION}/sobjects/{object_name}/'
 
        parent_record.pop('attributes')
        payload = parent_record.copy()
        payload['Name'] = 'test' + payload['Name']
        payload.pop('Id')
   
        try:
            response = requests.post(url, headers=headers, json=payload)
   
            if response.status_code == 201:
                print('Successfully created parent record')
                print(f'response: {response.json()}')
                return response.json()['id']
            else:
                print(f'Couldnt create parent record. Error: {response.json()}')
   
        except Exception as e:
            print(f'An error occured: ', str(e))
        pass
 
 
def main():
    try:
        source = SalesforceOrg(
            client_id='',
            client_secret='',
            username='',
            password='',
            auth_url='https://your-domain.my.salesforce.com/services/oauth2/token'
        )
 
        source.authenticate()
        parent_records_id_mapping = {}
        query = "SELECT AccountNumber, Active__c, AnnualRevenue, CleanStatus, CustomerPriority__c, Description, Id, Name, Phone from Account where Id = 'id'"
        parent_records = source.fetch_parent_record(query)
 
        query = "SELECT Id, Name, Description, ParentId, ContentType FROM Attachment where ParentId = 'id'"
        attachments = source.fetch_attachments(query)
        # print(f"Retrieved {len(attachments)} attachments.")
        # print(f'attachments info: {attachments}')
 
        for parent_record in parent_records:
            print(parent_record)
            parent_records_id_mapping[parent_record.get('Id')] = source.create_parent_record(parent_record)
 
            print(parent_records_id_mapping)
       
        # Save each attachment's content
        for idx, att in enumerate(attachments):
            filename, file_content = source.get_attachment_content(att)
            #print(f"Saved file: {att.get('Name')} [{idx + 1}/{len(attachments)}]")
            print(att)
            att['ParentId'] = parent_records_id_mapping[att['ParentId']]
 
            new_attachment_id = source.create_attachment(att, file_content)
            print(f'created attachment: {new_attachment_id}')
   
    except Exception as e:
        print(f"An error occurred: {str(e)}")
 
 
if __name__ == "__main__":
    main()
