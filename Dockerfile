FROM squidfunk/mkdocs-material:latest

RUN pip install mkdocs-swagger-ui-tag

EXPOSE 8000

ENTRYPOINT ["mkdocs", "serve", "--dev-addr=0.0.0.0:8000", "--livereload"]
