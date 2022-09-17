from cryptography.hazmat.primitives.serialization import load_pem_public_key


PUBKEY_PEM = """
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VuAyEACNBmeUgTDsfZhnueWfymEJO7YnaXJgZ8xcZY8q2seyk=
-----END PUBLIC KEY-----
"""

key_obj = load_pem_public_key(PUBKEY_PEM.encode('utf-8'))

pass