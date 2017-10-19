FROM python:3.6-alpine3.4 as builder

RUN apk add --no-cache gcc g++ make python gettext jq coreutils util-linux

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD [ "python", "/usr/src/app/processor" ]
