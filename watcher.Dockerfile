# syntax=docker/dockerfile:1

FROM node:lts-alpine
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]
ENV NODE_ENV=production
WORKDIR /app
COPY ["package.json", "yarn.lock*", "./"]
RUN yarn install --frozen-lockfile --production
COPY watcher.js ./
CMD [ "node", "watcher.js" ]
