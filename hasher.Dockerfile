# syntax=docker/dockerfile:1

FROM node:lts-buster
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]
RUN apt-get update && apt-get install -y ffmpeg openjdk-11-jre
ENV NODE_ENV=production
WORKDIR /app
COPY ["package.json", "package-lock.json*", "./"]
RUN npm install --production
COPY jar/ ./jar/
COPY hasher.js ./
CMD [ "node", "hasher.js" ]