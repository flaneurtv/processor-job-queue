FROM python:3.6-alpine3.4 as builder

ENV SERVICE_NAME=job_queue
WORKDIR /srv

COPY requirements.txt ./
RUN apk add --no-cache curl gcc g++ make

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
  && python get-pip.py \
  && rm get-pip.py \
  && pip install --no-cache-dir -r requirements.txt \
  && apk del --no-cache curl gcc g++ make

RUN apk add --no-cache gettext jq coreutils util-linux

COPY processor /srv/processor
COPY subscriptions.txt /srv/subscriptions.txt
COPY LICENSE /srv/.

COPY --from=flaneurtv/samm /usr/local/bin/samm /usr/local/bin/samm

CMD ["samm"]
