FROM bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8
RUN curl https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /wait-for-it.sh && \
    chmod u+x /wait-for-it.sh

COPY ["run_and_import.sh", "/run_and_import.sh"]
CMD ["/run_and_import.sh"]
