from google.cloud import storage

client = storage.Client()
buckets = list(client.list_buckets())
print("✅ Connection successful! Available Buckets:")
for bucket in buckets:
    print(bucket.name)


