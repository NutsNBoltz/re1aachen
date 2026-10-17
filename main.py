import requests
import io
from PIL import Image
from pip._internal.network import session

url = "https://api.transitous.org/api/v1/geocode"
parameters = {"text": "Aachen Hbf", "text": "Dortmund Hbf"}




headers = {
    # Public APIs appreciate an identifier, or you can use a browser-like string:
    "User-Agent": "RE1-Delay-Tracker/1.0 (Student Project: hasan.jilani@rwth-aachen.de)"
}

response = requests.get(url, params=parameters , headers=headers)


print(response.status_code)
print(response.json()[0])
print(type(response.json()[0]))
print(response.json()[0].keys())


latitudeAachen = response.json()[0].get('lat')
longitudeAachen = response.json()[0].get('lon')


