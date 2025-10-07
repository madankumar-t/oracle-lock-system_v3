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

# -------------------------------------------------------------------
# --------------------------- Helpers -------------------------------
# -------------------------------------------------------------------

def encode_tagging(tags: dict) -> str:
    """Build an S3 Tagging string like 'comment=My%20note&key2=val2'."""
    items = list(tags.items())[:10]
    pairs = []
    for k, v in items:
        k = str(k)[:128]
        v = str(v)[:256]
        pairs.append(f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}")
    return "&".join(pairs)


def tags_list_to_dict(tagset):
    return {t['Key']: t['Value'] for t in (tagset or [])}


def parse_bool(val):
    if isinstance(val, bool):
        return val
    if not isinstance(val, str):
        return False
    return val.lower() in ('1', 'true', 'yes', 'y', 'on')


def get_qp(event, key, default=None):
    qs = (event or {}).get('queryStringParameters') or {}
    return qs.get(key, default)


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

# -------------------------------------------------------------------
# ---------------------- Lambda Entry Point -------------------------
# -------------------------------------------------------------------

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    method = event.get('httpMethod', '')
    path = event.get('resource', event.get('path', ''))
    body = json.loads(event['body']) if event.get('body') else {}

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
        # ✅ NEW: generate presigned PUT/GET URLs for large file uploads
        return generate_presigned_url(body)
    elif path == '/s3-files' and method == 'GET':
        return list_s3_files(event)
    elif path == '/search' and method == 'GET':
        return search_s3_files(event)
    elif path == '/upload' and method == 'POST':
        # ⚠️ Deprecated for large files (use /get-url instead)
        return upload_files_to_s3(body)
    elif path == '/download' and method == 'POST':
        return download_file_from_s3(body)
    elif path == '/versions' and method == 'POST':
        return get_file_versions(body)
    elif path == '/tags' and method == 'POST':
        return get_object_tags(body)
    else:
        return build_response(400, {"error": f"Unsupported operation: {method} {path}"})


# -------------------------------------------------------------------
# --------------------- DynamoDB Operations -------------------------
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# ---------------------- Large File Upload --------------------------
# -------------------------------------------------------------------

def generate_presigned_url(body):
    """
    Request:
    {
      "filename": "path/to/file.txt",
      "action": "put" | "get",
      "content_type": "text/plain",
      "comment": "optional"
    }
    """
    try:
        filename = body['filename']
        action = body['action']
        content_type = body.get('content_type', 'application/octet-stream')
        comment = body.get('comment')

        if action == 'get':
            params = {'Bucket': BUCKET, 'Key': filename}
            url = s3.generate_presigned_url(
                'get_object',
                Params=params,
                ExpiresIn=3600  # 1 hour expiration
            )
            return build_response(200, {'url': url})

        elif action == 'put':
            params = {
                'Bucket': BUCKET,
                'Key': filename,
                'ContentType': content_type
            }

            headers = {'Content-Type': content_type}

            if comment:
                tagging_str = encode_tagging({'comment': comment})
                params['Tagging'] = tagging_str
                headers['x-amz-tagging'] = tagging_str

            url = s3.generate_presigned_url(
                'put_object',
                Params=params,
                ExpiresIn=3600  # 1 hour expiration
            )

            return build_response(200, {
                'url': url,
                'headers': headers
            })

        else:
            return build_response(400, {'error': "action must be 'get' or 'put'"})

    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})


# -------------------------------------------------------------------
# ------------------ Other S3 Operations (Unchanged) ----------------
# -------------------------------------------------------------------

def list_s3_files(event=None):
    try:
        include_tags = parse_bool(get_qp(event, 'include_tags', 'false'))
        prefix = get_qp(event, 'prefix', None)
        list_kwargs = {'Bucket': BUCKET}
        if prefix:
            list_kwargs['Prefix'] = prefix

        response = s3.list_objects_v2(**list_kwargs)
        contents = response.get('Contents', [])
        files = []

        for obj in contents:
            entry = {
                'key': obj['Key'],
                'size': obj.get('Size'),
                'last_modified': obj.get('LastModified').isoformat() if obj.get('LastModified') else None
            }
            if include_tags:
                try:
                    tags = s3.get_object_tagging(Bucket=BUCKET, Key=obj['Key'])
                    entry['tags'] = tags_list_to_dict(tags.get('TagSet', []))
                except Exception:
                    entry['tags'] = {'_error': 'failed_to_fetch'}
            files.append(entry)

        return build_response(200, {'items': files})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def upload_files_to_s3(body):
    """
    ⚠️ Legacy small-file upload (≤10 MB). Use /get-url for larger files.
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
    except Exception as e:
        return build_response(500, {'error': str(e)})


def download_file_from_s3(body):
    try:
        filename = body['filename']
        obj = s3.get_object(Bucket=BUCKET, Key=filename)
        content = obj['Body'].read()
        encoded = base64.b64encode(content).decode('utf-8')

        return build_response(200, {
            'filename': filename,
            'content_base64': encoded,
            'content_type': obj.get('ContentType', 'application/octet-stream')
        })
    except Exception as e:
        return build_response(500, {'error': str(e)})


def get_file_versions(body):
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
                        'Size': v['Size']
                    })

        versions.sort(key=lambda x: x['LastModified'], reverse=True)
        return build_response(200, {'filename': filename, 'versions': versions})
    except Exception as e:
        return build_response(500, {'error': str(e)})


def get_object_tags(body):
    try:
        filename = body['filename']
        resp = s3.get_object_tagging(Bucket=BUCKET, Key=filename)
        return build_response(200, {
            'filename': filename,
            'tags': tags_list_to_dict(resp.get('TagSet', []))
        })
    except Exception as e:
        return build_response(500, {'error': str(e)})
