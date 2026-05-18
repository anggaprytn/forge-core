FROM busybox:1.36

ENV PORT=3000

RUN mkdir -p /www \
    && printf 'ok\n' > /www/health \
    && printf 'forge-sample\n' > /www/index.html

CMD ["sh", "-c", "exec httpd -f -p ${PORT} -h /www"]
