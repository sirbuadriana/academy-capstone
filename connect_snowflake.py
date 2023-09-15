import botocore 
import botocore.session 
import json
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

def get_snowflake_credentials():
    client = botocore.session.get_session().create_client('secretsmanager')
    cache_config = SecretCacheConfig()
    cache = SecretCache( config = cache_config, client = client)
    secret = cache.get_secret_string('snowflake/capstone/login')
    return json.loads(secret)



if __name__ == "__main__":
    secret = get_snowflake_credentials()
    print(secret["DATABASE"])
    print(secret["PASSWORD"])