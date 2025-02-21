FROM quay.io/astronomer/astro-runtime:12.7.1



# Add arguments to be set at build time
ARG ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/keys/astro_test.json

