FROM python:3.10-slim

RUN pip install duckdb pytz

WORKDIR /analytics

#CMD ["python", "transform_scd2.py"]
ENTRYPOINT ["sh", "-c"]
CMD ["python transform_scd2.py && tail -f /dev/null"]

