config = {
    'hbase' : {
        # HBase hostname/IP
        'host': 'hbase'
    },
    'hdfs': {
        # Hadoop NameNode location
        'host': 'http://namenode',
        'port': 50070,
        # HDFS directory where files for import are placed
        'prefix': "/"
    },
    # Max number of records to import from each file.
    # set to negative value or None to import everything
    'limit': 100,
}