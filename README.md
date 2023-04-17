# shotit-worker

[![License](https://img.shields.io/github/license/shotit/shotit-worker.svg?style=flat-square)](https://github.com/shotit/shotit-worker/blob/master/LICENSE)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/shotit/shotit-worker/Docker%20Image%20CI?style=flat-square)](https://github.com/shotit/shotit-worker/actions)
[![Watcher Docker](https://img.shields.io/docker/pulls/shotit/shotit-worker-watcher?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-watcher)
[![Watcher Docker Image Size](https://img.shields.io/docker/image-size/shotit/shotit-worker-watcher/v0.9.1?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-watcher)
[![Hasher Docker](https://img.shields.io/docker/pulls/shotit/shotit-worker-hasher?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-hasher)
[![Hasher Docker Image Size](https://img.shields.io/docker/image-size/shotit/shotit-worker-hasher/v0.9.1?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-hasher)
[![Loader Docker](https://img.shields.io/docker/pulls/shotit/shotit-worker-loader?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-loader)
[![Loader Docker Image Size](https://img.shields.io/docker/image-size/shotit/shotit-worker-loader/v0.9.1?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-loader)
[![Searcher Docker](https://img.shields.io/docker/pulls/shotit/shotit-worker-searcher?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-searcher)
[![Searcher Docker Image Size](https://img.shields.io/docker/image-size/shotit/shotit-worker-searcher/v0.9.1?style=flat-square)](https://hub.docker.com/r/lesliewong007/shotit-worker-searcher)

Backend workers for [shotit](https://github.com/shotit/shotit). Four core workers of shotit: watcher, hasher, loader and searcher.

### Features

- watch file system changes and upload hash to shotit-media
- download video from shotit-media, hash, and upload to shotit-api
- download hash from shotit-api, deduplicate, and upload to solr

### Prerequisites

- Node.js 14.x
- ffmpeg 4.x
- java (openjdk 1.8.0)
- git
- [pm2](https://pm2.keymetrics.io/)

### Install

Install Prerequisites first, then:

```
git clone https://github.com/shotit/shotit-worker.git
cd shotit-worker
npm install
```

### Configuration

- Copy `.env.example` to `.env`
- Edit `.env` as appropriate for your setup

### Start workers

You can use pm2 to run this in background in cluster mode.

Use below commands to start / restart / stop server.

```
npm run start
npm run stop
npm run reload
npm run restart
npm run delete
```

To change the number of nodejs instances, edit ecosystem.config.json
