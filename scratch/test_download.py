import urllib.request
import os

url = "https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb/sample.csv"
output = "data/raw/test_downloaded.csv"

print(f"Downloading {url}...")
try:
    urllib.request.urlretrieve(url, output)
    print(f"Success! File size: {os.path.getsize(output)} bytes")
except Exception as e:
    print(f"Error: {e}")
