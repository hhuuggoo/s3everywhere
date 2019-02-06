from .utils import cached_property, to_bytes


class S3Bucket(object):
    def __init__(self, storage, name):
        self.storage = storage
        self.name = name

    def put_object(self, key, data):
        self.storage.client.put_object(
            ACL='private',
            Body=to_bytes(data),
            Bucket=self.name,
            Key=key
        )

    def get_object(self, key):
        return self.storage.client.get_object(
            Bucket=self.name,
            Key=key
        )['Body'].read()

    def delete_objects(self, *keys):
        self.storage.client.delete_objects(
            Bucket=self.name,
            Delete={
                'Objects': [{'Key': k} for k in keys]
            },
        )

    def _bucket(self):
        return self.storage.resource.Bucket(self.name)

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
        self.storage.client.delete_bucket(Bucket=self.name)


class S3Storage(object):
    def __init__(self, default_region='us-east-2'):
        self.region = default_region
        pass

    @cached_property
    def client(self):
        import boto3
        return boto3.client('s3')

    @cached_property
    def resource(self):
        import boto3
        return boto3.resource('s3')

    def create_bucket(self, name, acl='private'):
        self.client.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={
                'LocationConstraint': self.region
            },
            ACL=acl
        )
        bucket = S3Bucket(self, name)
        bucket._bucket().wait_until_exists()
        self.block_public_access(name)

    def block_public_access(self, name):
        self.client.put_public_access_block(
            Bucket=name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True,
            }
        )
