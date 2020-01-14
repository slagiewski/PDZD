config = {
    'hbase': {
        # HBase hostname/IP
        'host': 'hbase'
    },
    'hdfs': {
        # Hadoop NameNode location
        'host': 'http://namenode',
        'port': 50070,
        # HDFS directory where files for import are placed
        'prefix': "/user/datasources/"
    },
    # Max number of records to import from each file.
    # set to 0, negative value or None to import everything
    'limit': 0,
    'skip': { # how mayn rows to skip e.g because they're already loaded
        'business': 180000,
        'user': 0,
        'review': 0,
        'photo': 0,
        'checkin': 0,
        'tip': 0
    },
    'max_timeout': 1, # maxium delay (in seconds) after failed save to hbase
    'max_fails': 6 # max number of failed ssaves to hbase before aborting
}
