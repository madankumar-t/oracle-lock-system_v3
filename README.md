# 🗂️ Oracle File Lock Management Lambda

This AWS Lambda application provides **file locking, metadata management, and large-file upload support** for Oracle ERP file storage.  
It enables users to:
- Lock/unlock files stored in Amazon S3
- Track file lock states in DynamoDB
- Upload and download files
- Upload **large files (>10 MB)** using **pre-signed S3 URLs**
- List, search, and tag S3 files

---

## 🚀 Architecture Overview

**AWS Services Used:**
- **Amazon API Gateway** – provides REST endpoints for clients (browser or application)
- **AWS Lambda (Python 3.11)** – handles API requests, generates presigned URLs
- **Amazon S3** – stores uploaded ERP files
- **Amazon DynamoDB** – tracks file lock status and ownership

**Upload paths:**
| Upload Type | Path | Data Flow | File Size Limit |
|--------------|------|-----------|-----------------|
| Small files  | `/upload` | Client → API Gateway → Lambda → S3 | ≤ 10 MB |
| Large files  | `/get-url` | Client → Lambda (URL only) → Direct S3 PUT | ≤ 5 GB |

---

## 🧩 Project Structure

```
oracle-file-lock/
├── template.yaml           # AWS SAM deployment template
├── lambda/
│   ├── lambda_handler.py   # Main Lambda source code
│   └── __init__.py
```

---

## ⚙️ Prerequisites

| Tool | Description |
|------|--------------|
| [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) | For AWS authentication |
| [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) | To build and deploy the Lambda |
| [Python 3.11+](https://www.python.org/downloads/) | Lambda runtime and local testing |

Verify installation:
```bash
aws --version
sam --version
python3 --version
```

---

## 🔑 AWS Setup

1. Configure credentials:
   ```bash
   aws configure
   ```
2. Verify identity:
   ```bash
   aws sts get-caller-identity
   ```

3. (Optional) Create an S3 bucket for deployment artifacts:
   ```bash
   aws s3 mb s3://my-sam-artifacts-bucket
   ```

---

## 🧱 Build and Deploy

### 1️⃣ Build the SAM Application
```bash
sam build
```

### 2️⃣ Deploy (Guided Mode)
```bash
sam deploy --guided
```

When prompted:
```
Stack Name [sam-app]: oracle-file-lock
AWS Region [us-east-1]: us-east-1
Confirm changes before deploy [Y/n]: n
Allow SAM CLI IAM role creation [Y/n]: Y
Save arguments to configuration file [Y/n]: Y
```

Once completed, SAM outputs your API Gateway endpoint:
```
Outputs
--------------------------------------------------------------------------------
Key                 ApiEndpoint
Description         API Gateway endpoint URL
Value               https://xxxxxxxxxx.execute-api.us-east-1.amazonaws.com/Prod
--------------------------------------------------------------------------------
```

You can redeploy later using:
```bash
sam deploy
```

---

## 🪣 Configure S3 Bucket CORS

Because large uploads use **pre-signed URLs**, S3 must allow CORS access.

Create a file `cors.json`:
```json
{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "PUT"],
      "AllowedHeaders": ["*"],
      "ExposeHeaders": ["ETag"]
    }
  ]
}
```

Apply it:
```bash
aws s3api put-bucket-cors   --bucket dcli-oracle-erp-storage   --cors-configuration file://cors.json
```

---

## 🧪 Test Locally (Optional)

You can invoke the API locally before deploying:

```bash
sam local start-api
```

Then visit:
```
http://127.0.0.1:3000/get-url
```

or use curl:
```bash
curl -X POST   -H "Content-Type: application/json"   -d '{"filename": "test.txt", "action": "put"}'   http://127.0.0.1:3000/get-url
```

---

## 🧠 API Endpoints Summary

| Path | Method | Description |
|------|---------|-------------|
| `/lock` | POST | Lock a file (writes to DynamoDB) |
| `/unlock` | POST | Unlock a file |
| `/status` | GET | Check lock status |
| `/get-url` | POST | Get presigned S3 URL (for upload/download) |
| `/upload` | POST | Upload small files (≤10 MB) via API Gateway |
| `/download` | POST | Download file from S3 |
| `/s3-files` | GET | List S3 files (optional tag info) |
| `/versions` | POST | Get S3 file versions |
| `/tags` | POST | Get S3 object tags |
| `/search` | GET | Search S3 by key name or tag |

---

## 💻 Example: Upload Large File (100 MB)

Frontend (JavaScript):
```javascript
async function uploadLargeFile(file) {
  // Step 1: Request presigned URL
  const res = await fetch("https://<api-endpoint>/get-url", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filename: `uploads/${file.name}`,
      action: "put",
      content_type: file.type
    }),
  });

  const { url, headers } = await res.json();

  // Step 2: Upload directly to S3
  await fetch(url, {
    method: "PUT",
    headers,
    body: file
  });

  console.log("✅ Upload complete:", file.name);
}
```

---

## 🧹 Cleanup

To delete all deployed resources (Lambda, API Gateway, DynamoDB):

```bash
sam delete
```

---

## 🧾 License

This project is licensed under the MIT License.  
See [LICENSE](LICENSE) for details.
