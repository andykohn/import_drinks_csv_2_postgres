import base64

import boto3
from Crypto.Cipher import AES
import aws_encryption_sdk


kms_key_provider = aws_encryption_sdk.KMSMasterKeyProvider(key_ids=[
    'arn:aws:kms:us-east-1:572226911665:key/952425ce-9f4b-4ffb-a8a5-cf8a3afadba0'
])


class KMSEncryptDecrypt:

    @staticmethod
    def encrypt_data(data):

        my_ciphertext, encryptor_header = aws_encryption_sdk.encrypt(
            source=data,
            key_provider=kms_key_provider
        )
        # encrypted_b64 = base64.b64encode(my_ciphertext)
        return my_ciphertext

    @staticmethod
    def decrypt_data(data):

        decrypted_plaintext, decryptor_header = aws_encryption_sdk.decrypt(
            source=data,
            key_provider=kms_key_provider
        )
        return decrypted_plaintext
