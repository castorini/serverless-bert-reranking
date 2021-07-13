import requests

data = {
        'docs': [{"contents": "Brian Killian/WireImage. Paula Deen and her brother Earl W. \u00e2\u0080\u009cBubba\u00e2\u0080\u009d Hiers are being sued by a former general manager at Uncle Bubba\u00e2\u0080\u0099s Seafood and Oyster House, a restaurant they co-own.".encode('latin1').decode('utf8'), "id": "7187155"}],
        'query': "what is paula deen's brother"
        }

r = requests.post('https://zcjb9x6wx5.execute-api.us-east-2.amazonaws.com/Prod/rerank', json=data)

print(r.json())
