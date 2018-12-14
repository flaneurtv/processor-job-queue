FROM python:3.6-alpine3.4 as builder

ENV SERVICE_NAME=melted-controller
WORKDIR /srv

RUN apk add --no-cache gcc g++ make python gettext jq coreutils util-linux

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY processor /srv/processor
COPY subscriptions.txt /srv/subscriptions.txt
COPY LICENSE /srv/.

COPY --from=flaneurtv/samm /usr/local/bin/samm /usr/local/bin/samm

CMD ["samm"]
