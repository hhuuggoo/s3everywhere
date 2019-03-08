from .utils import cached_property, to_bytes
from botocore.client import ClientError

class S3Bucket(object):
    def __init__(self, storage, name):
        self.storage = storage
        self.name = name

    def get_url(self, key):
        url = self.storage._client.generate_presigned_url(
            'get_object', Params={'Bucket': self.name, 'Key': key}, ExpiresIn=3600
        )
        return url

    def put_object(self, key, data, acl='private'):
        self.storage._client.put_object(
            ACL=acl,
            Body=to_bytes(data),
            Bucket=self.name,
            Key=key
        )

    def acl(self, key, acl='private'):
        self._bucket().Object(key).Acl().put(ACL='public-read')

    def get_object(self, key):
        return self.storage._client.get_object(
            Bucket=self.name,
            Key=key
        )['Body'].read()

    def delete_objects(self, *keys):
        self.storage._client.delete_objects(
            Bucket=self.name,
            Delete={
                'Objects': [{'Key': k} for k in keys]
            },
        )

    def _bucket(self):
        return self.storage._resource.Bucket(self.name)

    def keys(self):
        bucket = self._bucket()
        keys = [x.key for x in bucket.objects.all()]
        return keys

    def delete_all_objects(self):
        keys = self.keys()
        if len(keys) == 0:
            return
        self.delete_objects(*keys)

    def delete(self):
        self.storage._client.delete_bucket(Bucket=self.name)


class S3Storage(object):
    def __init__(self, default_region='us-east-2'):
        self.region = default_region
        pass

    def bucket(self, name):
        return S3Bucket(self, name)

    @cached_property
    def _client(self):
        import boto3
        from botocore.client import Config
        return boto3.client('s3',
                            self.region,
                            config=Config(s3={'addressing_style': 'path'}))

    @cached_property
    def _resource(self):
        import boto3
        return boto3.resource('s3')

    def exists(self, name):
        try:
            self._resource.meta.client.head_bucket(Bucket=name)
        except ClientError:
            return False
        else:
            return True

    def create_bucket(self, name, acl='private', block=True):
        self._client.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={
                'LocationConstraint': self.region
            },
            ACL=acl
        )
        bucket = S3Bucket(self, name)
        bucket._bucket().wait_until_exists()
        if block:
            self._block_public_access(name)

    def _block_public_access(self, name):
        self._client.put_public_access_block(
            Bucket=name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True,
            }
        )

    def list(self):
        buckets = [x['Name'] for x in self._client.list_buckets()['Buckets']]
        return buckets


class MockS3Bucket(S3Bucket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._data = {}

    def get_url(self, key):
        return ""

    def put_object(self, key, data):
        self._data[key] = data

    def get_object(self, key):
        return self._data[key]

    def delete_objects(self, *keys):
        for k in keys:
            self._data.pop(k)

    def keys(self):
        return self._data.keys()

    def delete_all_objects(self):
        self._data.clear()

    def delete(self):
        self.storage._data.pop(self.name)


class MockS3Storage(object):
    def __init__(self):
        self._data = {}

    def bucket(self, name):
        if name not in self._data:
            self.create_bucket(name)
        return self._data[name]

    def exists(self, name):
        return name in self._data

    def create_bucket(self, name, acl='private'):
        self._data[name] = MockS3Bucket(self, name)
        return self._data[name]

    def list(self):
        return self._data.keys()
