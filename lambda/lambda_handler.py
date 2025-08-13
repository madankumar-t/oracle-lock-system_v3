import boto3
import json
import os
import base64
from datetime import datetime

s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb')
table = ddb.Table(os.environ['DDB_TABLE'])
BUCKET = os.environ['S3_BUCKET']

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
        return generate_presigned_url(body)
    elif path == '/s3-files' and method == 'GET':
        return list_s3_files()
    elif path == '/upload' and method == 'POST':
        return upload_files_to_s3(body)
    elif path == '/download' and method == 'POST':
        # NEW: supports optional version
        return download_file_from_s3(body)
    elif path == '/versions' and method == 'POST':
        # Lists versions for a single key
        return get_file_versions(body)
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
      "version_id": "<optional VersionId>"
    }
    """
    try:
        filename = body['filename']
        action = body['action']
        version_id = body.get('version_id') or body.get('versionId')

        if action == 'get':
            params = {'Bucket': BUCKET, 'Key': filename}
            if version_id:
                params['VersionId'] = version_id
            url = s3.generate_presigned_url(
                'get_object', Params=params, ExpiresIn=3600
            )
        elif action == 'put':
            url = s3.generate_presigned_url(
                'put_object',
                Params={'Bucket': BUCKET, 'Key': filename},
                ExpiresIn=3600
            )
        else:
            return build_response(400, {'error': "action must be 'get' or 'put'"})

        return build_response(200, {'url': url})
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})

def list_s3_files():
    try:
        # Lists current objects (latest versions only)
        response = s3.list_objects_v2(Bucket=BUCKET)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        return build_response(200, files)
    except Exception as e:
        return build_response(500, {'error': str(e)})

def upload_files_to_s3(body):
    try:
        uploaded = []
        for file in body.get('files', []):
            key = file['key']
            content = base64.b64decode(file['content_base64'])
            content_type = file.get('content_type', 'application/octet-stream')

            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=content,
                ContentType=content_type
            )
            uploaded.append(key)

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
            # (Optional) include delete markers if you care
            # for dm in page.get('DeleteMarkers', []):
            #     if dm['Key'] == filename:
            #         versions.append({
            #             'VersionId': dm['VersionId'],
            #             'IsLatest': dm['IsLatest'],
            #             'LastModified': dm['LastModified'].isoformat(),
            #             'DeleteMarker': True
            #         })

        # Sort newest first (just in case)
        versions.sort(key=lambda x: x['LastModified'], reverse=True)

        return build_response(200, {'filename': filename, 'versions': versions})
    except KeyError as ke:
        return build_response(400, {'error': f"Missing field: {ke}"})
    except Exception as e:
        return build_response(500, {'error': str(e)})
