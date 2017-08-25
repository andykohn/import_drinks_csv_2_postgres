from helpers.KMS_encrypt_decrypt import KMSEncryptDecrypt
import boto3


def save_to_dynamodb(text):
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1',
                              endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('credentials')
    response = table.update_item(
        Key={
            'name': 'rds-drinks_db-drinks_user',
        },
        UpdateExpression='SET encrypted_password = :val1',
        ExpressionAttributeValues={
            ':val1': text
        }
    )


def __get_password_from_dynamo():
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1',
                              endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('credentials')
    response = table.get_item(
        Key={
            'name': 'rds-drinks_db-drinks_user',
        }
    )
    if 'Item' in response:
        if 'encrypted_password' in response['Item']:
            return response['Item']['encrypted_password']
        else:
            return "No such attribute : encrypted_password"
    else:
        return "No key found"


text = "REMOVED"

encrypted = KMSEncryptDecrypt.encrypt_data(text)
print(encrypted)
save_to_dynamodb(encrypted)
decrypted = KMSEncryptDecrypt.decrypt_data(encrypted)
print(decrypted)

encrypted_from_dynamodb = __get_password_from_dynamo()
decrypted = KMSEncryptDecrypt.decrypt_data(encrypted_from_dynamodb.value)
print(decrypted)