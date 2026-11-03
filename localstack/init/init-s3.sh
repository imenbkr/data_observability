#!/bin/bash
# Runs inside LocalStack once it is ready: creates the data-lake bucket.
awslocal s3 mb s3://plant-lake 2>/dev/null || true
