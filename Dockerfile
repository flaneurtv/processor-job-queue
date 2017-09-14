FROM node:alpine as builder

RUN apk add --no-cache gcc g++ make python
RUN yarn install
