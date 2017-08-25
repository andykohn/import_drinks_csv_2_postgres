import boto3
import psycopg2
from botocore.exceptions import ClientError
from lib.KMS_encrypt_decrypt import KMSEncryptDecrypt

HOST = 'drinks-db.ce6hlg7jz41w.us-east-1.rds.amazonaws.com'
DATABASE = 'drinks_db'
USER = 'drinks_user'
PORT = '8200'

CREATE_TABLE_INGREDIENTS = "CREATE TABLE IF NOT EXISTS ingredients (" + \
                     "ingredient_id INT PRIMARY KEY NOT NULL," + \
                     "ingredient_name text NOT NULL" + \
                     ");"

CREATE_TABLE_DRINKS = "CREATE TABLE IF NOT EXISTS drinks ( " + \
                "drink_id INT PRIMARY KEY NOT NULL, drink_name text, " + \
                "drink_category text,drink_alcoholic text, " + \
                "drink_glass text, drink_instructions text, drink_thumbnail text, drink_date_modified text " + \
                ");"

CREATE_TABLE_MAP_DRINK_INGREDIENTS = "CREATE TABLE IF NOT EXISTS map_drink_ingredients (" + \
                              "drink_id INT NOT NULL, " + \
                              "ingredient_id text NOT NULL, " + \
                              "measurement text, " + \
                              "ingredient_order text NOT NULL," + \
                              "PRIMARY KEY(drink_id, ingredient_id)" + \
                              ");"

DROP_TABLE_INGREDIENTS = "DROP TABLE IF EXISTS ingredients;"
DROP_TABLE_DRINKS = "DROP TABLE IF EXISTS drinks;"
DROP_TABLE_MAP_DRINK_INGREDIENTS = "DROP TABLE IF EXISTS map_drink_ingredients;"

# COPY_CSV_INGREDIENTS = "\copy ingredients(ingredient_id, ingredient_name) FROM './csv/ingredients.csv' DELIMITER ',' CSV HEADER;"
# COPY_CSV_MAP_DRINK = "\copy drinks(drink_id, drink_name, drink_category, drink_alcoholic, drink_glass, drink_instructions, drink_thumbnail, drink_date_modified) FROM './csv/drinks.csv' DELIMITER ',' CSV HEADER;"
# COPY_CSV_MAP_DRINK_INGREDIENTS = "\copy map_drink_ingredients(drink_id, ingredient_id, measurement, ingredient_order) FROM './csv/map_drink_ingredients.csv' DELIMITER ',' CSV HEADER;"
INGREDIENTS_CSV = './csv/ingredients.csv'
INGREDIENTS_TABLE = 'ingredients'
INGREDIENTS_COLUMNS = 'ingredient_id, ingredient_name'
DRINKS_CSV = './csv/drinks.csv'
DRINKS_TABLE = 'drinks'
DRINKS_COLUMNS = 'drink_id, drink_name, drink_category, drink_alcoholic, ' + \
                  'drink_glass, drink_instructions, drink_thumbnail, drink_date_modified'
MAP_DRINK_INGREDIENTS_CSV = './csv/map_drink_ingredients.csv'
MAP_DRINK_INGREDIENTS_TABLE = 'map_drink_ingredients'
MAP_DRINK_INGREDIENTS_COLUMNS = 'drink_id, ingredient_id, measurement, ingredient_order'


def __execute_sql(conn, sql):
    try:
        print("Processing {}".format(sql))
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    except Exception as error:
        print(error)


def __get_csv_files_from_s3():
    s3 = boto3.resource('s3')

    try:
        s3.Bucket('drinkslistvir').download_file('csv/drinks.csv', DRINKS_CSV)
        s3.Bucket('drinkslistvir').download_file('csv/ingredients.csv', INGREDIENTS_CSV)
        s3.Bucket('drinkslistvir').download_file('csv/map_drink_ingredients.csv', MAP_DRINK_INGREDIENTS_CSV)
    except ClientError as error:
        if error.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def __execute_psql_copy(conn, csv, table_name, columns_name):
    try:
        print("Coping {}".format(table_name))
        cur = conn.cursor()
        f = open(csv)
        statement = "copy {}({}) FROM STDIN DELIMITER ',' CSV HEADER;".format(table_name, columns_name)
        cur.copy_expert(statement, file=f)
        conn.commit()
        cur.close()
    except Exception as error:
        print(error)


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
            return response['Item']['encrypted_password'].value
        else:
            return "No such attribute : encrypted_password"
    else:
        return "No key found"


# TODO: Critical risk, although it's only a playground,
# nothing in the DB, and only internal IAM roles can access it, we need to still use KMS to encrypt this.
def __get_credential():
    try:
        password_encrypted = __get_password_from_dynamo()
    except ClientError as error:
        print(error.response['Error']['Message'])
        raise
    else:
        password = KMSEncryptDecrypt.decrypt_data(password_encrypted)
        return password
        # return response[u'Item'][u'value']


def __connect():
    credential = __get_credential()
    credential = credential.decode("utf-8")
    conn = psycopg2.connect(host=HOST, database=DATABASE, port=PORT, user=USER, password=credential)
    # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

__conn = __connect()
__execute_sql(__conn, DROP_TABLE_INGREDIENTS)
__execute_sql(__conn, DROP_TABLE_DRINKS)
__execute_sql(__conn, DROP_TABLE_MAP_DRINK_INGREDIENTS)

__execute_sql(__conn, CREATE_TABLE_INGREDIENTS)
__execute_sql(__conn, CREATE_TABLE_DRINKS)
__execute_sql(__conn, CREATE_TABLE_MAP_DRINK_INGREDIENTS)

__get_csv_files_from_s3()
__execute_psql_copy(__conn, INGREDIENTS_CSV, INGREDIENTS_TABLE, INGREDIENTS_COLUMNS)
__execute_psql_copy(__conn, DRINKS_CSV, DRINKS_TABLE, DRINKS_COLUMNS)
__execute_psql_copy(__conn, MAP_DRINK_INGREDIENTS_CSV, MAP_DRINK_INGREDIENTS_TABLE, MAP_DRINK_INGREDIENTS_COLUMNS)
__conn.close()