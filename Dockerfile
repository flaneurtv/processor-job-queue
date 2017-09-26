FROM node:alpine as builder

RUN apk add --no-cache gcc g++ make python gettext
RUN yarn install
