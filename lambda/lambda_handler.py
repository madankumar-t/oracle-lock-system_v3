import boto3
import json
import os
import base64
from datetime import datetime
from urllib.parse import quote

s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb')
table = ddb.Table(os.environ['DDB_TABLE'])
BUCKET = os.environ['S3_BUCKET']


# --- helpers ---
def encode_tagging(tags: dict) -> str:
    """
    Build an S3 Tagging string like 'comment=My%20note&key2=val2'
    Values are RFC3986-encoded with quote(). Max 10 tags enforced.
    """
    items = list(tags.items())[:10]
    pairs = []
    for k, v in items:
        k = str(k)[:128]   # S3 tag key limit
        v = str(v)[:256]   # S3 tag value limit
        pairs.append(
            f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}"
        )
    return "&".join(pairs)


def tags_list_to_dict(tagset):
    """Convert [{'Key': 'k', 'Value': 'v'}, ...] -> {'k': 'v', ...}"""
    return {t['Key']: t['Value'] for t in (tagset or [])}


def parse_bool(val):
    if isinstance(val, bool):
        return val
    if not isinstance(val, str):
        return False
    return val.lower() in ('1', 'true', 'yes', 'y', 'on')


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    method = event.get('httpMethod', '')
    path = event.get('resource', event.get('path', ''))
    body = json.loads(event['body']) if event.get('body') else {}

    # CORS preflight
    if method == 'OPTIONS':
        return build_response(200)

    # Routes
    if path in ['/list', '/status'] and method == 'GET':
        return list_files()
    elif path == '/lock' and method == 'POST':
        return lock_file(body)
    elif path == '/unlock' and method == 'POST':
        return unlock_file(body)
    elif path == '/get-url' and method == 'POST':
        # supports tagging for presigned PUT
        return generate_presigned_url(body)
    elif path == '/s3-files' and method == 'GET':
        # supports include_tags via query string (?include_tags=true)
        return list_s3_files(event)
    elif path == '/upload' and method == 'POST':
        # adds tagging on upload when "comment" provided
        return upload_files_to_s3(body)
    elif path == '/download' and method == 'POST':
        return download_file_from_s3(body)
    elif path == '/versions' and method == 'POST':
        return get_file_versions(body)
    elif path == '/tags' and method == 'POST':
        # fetch tags for a single object (and optional VersionId)
        return get_object_tags(body)
    else:
        return build_response(400, {"error": f"Unsupported operation: {method} {path}"})


def build_response(status, body=None):
    return {
        'statusCode': status,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body) if body is not None else ''
    }


def list_files():
    try:
        resp = table.scan()
        return build_response(200, resp.get('Items', []))
    except Exception as e:
        return build_response(500, {'error': str(e)})


def lock_file(body):
    try:
        filename = body['filename']
        user = body['user']
        now = datetime.utcnow().isoformat()

        resp = table.get_item(Key={'filename': filename})
        if 'Item' in resp and resp['Item'].get('status') == 'locked':
            return build_response(409, 'File is already locked')

        table.put_item(Item={
            'filename': filename,
            'status': 'locked',
            'locked_by': user,
            'timestamp': now
        })
        return build_response(200, 'File locked')
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def unlock_file(body):
    try:
        filename = body['filename']
        table.update_item(
            Key={'filename': filename},
            UpdateExpression='SET #s = :s REMOVE locked_by',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':s': 'unlocked'}
        )
        return build_response(200, 'File unlocked')
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def generate_presigned_url(body):
    """
    Request:
    {
      "filename": "path/to/file.txt",
      "action": "get" | "put",
      "version_id": "<optional VersionId>",
      "comment": "optional comment to store as S3 tag on PUT"
    }
    Response for PUT includes required 'headers' to send (x-amz-tagging).
    """
    try:
        filename = body['filename']
        action = body['action']
        version_id = body.get('version_id') or body.get('versionId')
        comment = body.get('comment')  # optional

        if action == 'get':
            params = {'Bucket': BUCKET, 'Key': filename}
            if version_id:
                params['VersionId'] = version_id
            url = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=3600)
            return build_response(200, {'url': url})

        elif action == 'put':
            params = {'Bucket': BUCKET, 'Key': filename}
            required_headers = {}

            # If comment present, sign the Tagging into the URL and return the header client must send.
            if comment:
                tagging_str = encode_tagging({'comment': comment})
                params['Tagging'] = tagging_str
                required_headers['x-amz-tagging'] = tagging_str

            url = s3.generate_presigned_url('put_object', Params=params, ExpiresIn=3600)
            resp = {'url': url}
            if required_headers:
                resp['headers'] = required_headers
            return build_response(200, resp)

        else:
            return build_response(400, {'error': "action must be 'get' or 'put'"})

    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def list_s3_files(event=None):
    try:
        include_tags = False
        if event and event.get('queryStringParameters'):
            include_tags = parse_bool(event['queryStringParameters'].get('include_tags', 'false'))

        response = s3.list_objects_v2(Bucket=BUCKET)
        contents = response.get('Contents', [])

        if not include_tags:
            files = [obj['Key'] for obj in contents]
            return build_response(200, files)

        files = []
        for obj in contents:
            key = obj['Key']
            try:
                tag_resp = s3.get_object_tagging(Bucket=BUCKET, Key=key)
                tags = tags_list_to_dict(tag_resp.get('TagSet', []))
            except Exception:
                tags = {'_error': 'failed_to_fetch'}
            files.append({'key': key, 'tags': tags})

        return build_response(200, files)

    except Exception as e:
        return build_response(500, {'error': str(e)})


def upload_files_to_s3(body):
    """
    Request:
    {
      "files": [
        {
          "key": "path/to/file.txt",
          "content_base64": "...",
          "content_type": "text/plain",
          "comment": "optional comment to store as S3 tag"
        }
      ]
    }
    """
    try:
        uploaded = []
        for file in body.get('files', []):
            key = file['key']
            content = base64.b64decode(file['content_base64'])
            content_type = file.get('content_type', 'application/octet-stream')

            put_kwargs = {
                'Bucket': BUCKET,
                'Key': key,
                'Body': content,
                'ContentType': content_type
            }

            # Add Tagging on create if comment provided
            comment = file.get('comment')
            if comment:
                put_kwargs['Tagging'] = encode_tagging({'comment': comment})

            result = s3.put_object(**put_kwargs)
            uploaded.append({
                'key': key,
                'etag': result.get('ETag'),
                'version_id': result.get('VersionId')
            })

        return build_response(200, {'uploaded': uploaded})
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def download_file_from_s3(body):
    """
    Request:
    {
      "filename": "path/to/file.txt",
      "version_id": "<optional VersionId>"
    }
    If version_id omitted, downloads the latest.
    """
    try:
        filename = body['filename']
        version_id = body.get('version_id') or body.get('versionId')

        params = {'Bucket': BUCKET, 'Key': filename}
        if version_id:
            params['VersionId'] = version_id

        obj = s3.get_object(**params)
        content = obj['Body'].read()
        encoded = base64.b64encode(content).decode('utf-8')

        return build_response(200, {
            'filename': filename,
            'version_id': obj.get('VersionId', version_id),
            'content_base64': encoded,
            'content_type': obj.get('ContentType', 'application/octet-stream'),
            'etag': obj.get('ETag')
        })
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except s3.exceptions.NoSuchKey:
        return build_response(404, {'error': 'Object not found'})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def get_file_versions(body):
    """
    Request:
    { "filename": "path/to/file.txt" }
    Response includes all versions for that exact Key, newest first.
    """
    try:
        filename = body['filename']

        paginator = s3.get_paginator('list_object_versions')
        pages = paginator.paginate(Bucket=BUCKET, Prefix=filename)

        versions = []
        for page in pages:
            for v in page.get('Versions', []):
                if v['Key'] == filename:
                    versions.append({
                        'VersionId': v['VersionId'],
                        'IsLatest': v['IsLatest'],
                        'LastModified': v['LastModified'].isoformat(),
                        'Size': v['Size'],
                        'ETag': v.get('ETag')
                    })

        versions.sort(key=lambda x: x['LastModified'], reverse=True)
        return build_response(200, {'filename': filename, 'versions': versions})
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def get_object_tags(body):
    """
    Request:
    {
      "filename": "path/to/file.txt",
      "version_id": "<optional VersionId>"
    }
    Response:
    {
      "filename": "...",
      "version_id": "<if present>",
      "tags": { "comment": "...", ... }
    }
    """
    try:
        filename = body['filename']
        version_id = body.get('version_id') or body.get('versionId')

        params = {'Bucket': BUCKET, 'Key': filename}
        if version_id:
            params['VersionId'] = version_id

        resp = s3.get_object_tagging(**params)
        return build_response(200, {
            'filename': filename,
            'version_id': version_id,
            'tags': tags_list_to_dict(resp.get('TagSet', []))
        })
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except s3.exceptions.NoSuchKey:
        return build_response(404, {'error': 'Object not found'})
    except Exception as e:
        return build_response(500, {'error': str(e)})
