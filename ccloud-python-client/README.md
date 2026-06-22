# Python Client

This project contains a Python 3 application that subscribes to a topic on a Confluent Cloud Kafka cluster and sends a sample message, then consumes it and prints the consumed record to the console.

## Prerequisites

We assume that you already have Python 3 installed. The template was last tested against Python 3.12.5.

The instructions use `virtualenv` but you may use other virtual environment managers like `venv` if you prefer.

Create Confluent Cloud account follow the below link

```https://login.confluent.io/login?state=hKFo2SB6UndSdkZKY1pPVkZnbkhIOUR1cU5sRFhpTVJfRmVib6FupWxvZ2luo3RpZNkgWll5ZTVzcll4MjlvRHlYdWZ5ZmVtaFJsb2VYTVZTel-jY2lk2SBsMmhPcDBTMHRrU0IwVEZ0dklZZlpaOUVhS0Z2clNjNg&client=l2hOp0S0tkSB0TFtvIYfZZ9EaKFvrSc6&protocol=oauth2&prompt=login&audience=https%3A%2F%2Fconfluent.cloud%2Fapi&useRefreshTokensFallback=true&redirect_uri=https%3A%2F%2Fconfluent.cloud%2Fauth_callback&redirect_path=%2F&last_org_resource_id_map=%7B%2231d80a8475e396bec1b78cac871ec2359400fa4d32d49be98362db28b91c30d7%22%3A%7B%22org_resource_id%22%3A%22801ae530-567d-4e49-b90c-2596802f54c9%22%2C%22timestamp%22%3A1782072325281%2C%22is_sso%22%3Afalse%7D%7D&segment_anon_id=5c00a1a3-3ffb-49ba-add8-2dfaea629254&scope=openid%20profile%20email%20offline_access&response_type=code&response_mode=query&nonce=aDFBaUttOHl0WGlLYU5fTVlHNTBjektuOHBkMFBBdU94S3BLZGU3NzlaSQ%3D%3D&code_challenge=SJ7GRwUvWg10Wyx6WWY0eQCuORhVlmV4ya9sESZASP0&code_challenge_method=S256&auth0Client=eyJuYW1lIjoiYXV0aDAtcmVhY3QiLCJ2ZXJzaW9uIjoiMS4xMi4xIn0%3D```

## Installation

Create and activate a Python environment, so that you have an isolated workspace:

```shell
virtualenv env
source env/bin/activate
```

Install the dependencies of this application:

```shell
pip3 install -r requirements.txt
```

## Usage

You can execute the consumer script by running:

```shell
python3 client.py
```

## Troubleshooting

### Running `pip3 install -r requirements.txt` fails

If the execution of `pip3 install -r requirements.txt` fails with an error message indicating that librdkafka cannot be
found, please check if you are using a Python version for which a
[built distribution](https://pypi.org/project/confluent-kafka/2.3.0/#files) of `confluent-kafka` is available.


## Learn more

- For the Python client API, check out the [kafka-clients documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- Check out the full [getting started tutorial](https://developer.confluent.io/get-started/python/)
