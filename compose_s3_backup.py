#! /usr/bin/env python
"""Compose API Backup to S3 Utility.
Uses the Compose (MongoHQ) API to pull the latest backup for a database, and put the file on Amazon S3

Usage:
mongoHQ_S3_backup.py -d <database_name> -t <oauthToken> -a <account_name> -b <bucket> -k <aws_key_id> -s <aws_secret> -p <s3_key_prefix>
mongoHQ_S3_backup.py (-h | --help)

Options:
-h --help      Show this screen.
-d <database_name> --database=<database_name>  Name of the database to find a backup for.
-t <oauth_token> --token=<oauth_token>         MongoHQ OAUTH Token
-a <account_name> --account=<account_name>     MongoHQ Account Name
-b <bucket> --bucket=<bucket>                  S3 Bucket name
-k <aws_key_id> --awskey=<aws_key_id>          AWS Key ID
-s <aws_secret> --awssecret=<aws_secret>       AWS Secret Key
-p <s3_key_prefix> --prefix=<s3_key_prefix     Prefixes filename of S3 object [default: '']
"""
import requests
import math
import os
import sys
from docopt import docopt
import boto
from filechunkio import FileChunkIO

# Compose/MongoHQ API docs
# http://support.mongohq.com/rest-api/2014-06/backups.html


# Gets the latest backup for a given database and account.
def get_backup(database_name, account_name, oauth_token):
    mongohq_url = 'https://api.mongohq.com/accounts/{0}/backups'.format(account_name)
    headers = {'Accept-Version': '2014-06', 'Content-Type': 'application/json',
               'Authorization': 'Bearer {0}'.format(oauth_token)}
    # get the list of backups for our account.
    r = requests.get(mongohq_url, headers=headers)

    if r.status_code != 200:
        print 'Unable to list backups!'
        return None

    all_backups = r.json()
    backups_for_this_database = list()
    for backup in all_backups:
        if database_name in backup['database_names']:
            backups_for_this_database.append(
                {'id': backup['id'], 'created_at': backup['created_at'], 'filename': backup['filename']})

    # search for the latest backup for the given database name
    latest = sorted(backups_for_this_database, key=lambda k: k['created_at'])[-1]
    print 'The latest backup for {0} is: {1} created at {2}'.format(database_name, latest['id'], latest['created_at'])
    backup_filename = latest['filename']

    # pull down the backup
    r2 = requests.get('{0}/{1}/download'.format(mongohq_url, latest['id']), headers=headers, allow_redirects=False)
    if r2.status_code != 302:
        return None
    # MongoHQ backup API redirects to a URL where the backup file can be downloaded.
    # TODO: Can the 302 be followed in one step?
    file_location = r2.headers['location']

    # download the file to disk. Stream, since the file could potentially be large
    print 'Downloading Backup from:{0}'.format(file_location)
    r3 = requests.get(file_location, stream=True)
    with open(backup_filename, 'wb') as f:
        for chunk in r3.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    print 'saved backup to file: {0}'.format(backup_filename)
    return backup_filename


# Using S3 Multipart upload to handle potentially large files
def upload_to_s3(s3key, filename, bucket, aws_key, aws_secret):
    conn = boto.connect_s3(aws_key, aws_secret)
    bucket = conn.get_bucket(bucket)
    # Get file info
    source_path = filename
    source_size = os.stat(source_path).st_size
    # Create a multipart upload request
    mp = bucket.initiate_multipart_upload(s3key)
    # Use a chunk size of 50 MiB
    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / chunk_size))
    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size.
    for i in range(chunk_count + 1):
        print 'Uploading file chunk: {0} of {1}'.format(i + 1, chunk_count + 1)
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)
    # Finish the upload
    mp.complete_upload()
    return 0


def delete_local_backup_file(filename):
    print 'Deleting file from local filesystem:{0}'.format(filename)
    os.remove(filename)


if __name__ == '__main__':
    # grab all the arguments
    arguments = docopt(__doc__, version='mongoHQ_s3_backup 0.0.1')
    database_name = arguments['--database']
    account_name = arguments['--account']
    oauth_token = arguments['--token']
    bucket = arguments['--bucket']
    aws_key = arguments['--awskey']
    aws_secret = arguments['--awssecret']
    prefix = arguments['--prefix']

    # first, fetch the backup
    filename = get_backup(database_name, account_name, oauth_token)
    if filename is None:
        # we failed to save the backup successfully.
        sys.exit(1)
    # now, store the file we just downloaded up on S3
    print 'Uploading file to S3. Bucket:{0}'.format(bucket)
    s3_success = upload_to_s3(prefix + filename, filename, bucket, aws_key, aws_secret)
    if s3_success is None:
        #somehow failed the file upload
        print 'Failure with S3 upload. Exiting...'
        sys.exit(1)
    print 'Upload to S3 completed successfully'
    #Delete the local backup file, to not take up excessive disk space
    delete_local_backup_file(filename)


