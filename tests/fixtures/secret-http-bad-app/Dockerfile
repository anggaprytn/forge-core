FROM busybox:1.36

ENV PORT=3000

RUN mkdir -p /www

CMD ["sh", "-c", "test -n \"$DATABASE_URL\" && printf 'present\\n' > /www/secret-present && exec httpd -f -p ${PORT} -h /www"]
