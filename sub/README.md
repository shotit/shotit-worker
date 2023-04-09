# shotit-worker

[![License](https://img.shields.io/github/license/soruly/shotit-worker.svg?style=flat-square)](https://github.com/soruly/shotit-worker/blob/master/LICENSE)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/soruly/shotit-worker/Docker%20Image%20CI?style=flat-square)](https://github.com/soruly/shotit-worker/actions)
[![Docker](https://img.shields.io/docker/pulls/soruly/shotit-worker-hasher?style=flat-square)](https://hub.docker.com/r/soruly/shotit-worker-hasher)
[![Docker Image Size](https://img.shields.io/docker/image-size/soruly/shotit-worker-hasher/latest?style=flat-square)](https://hub.docker.com/r/soruly/shotit-worker-hasher)
[![Discord](https://img.shields.io/discord/437578425767559188.svg?style=flat-square)](https://discord.gg/K9jn6Kj)

Backend workers for [shotit](https://github.com/soruly/shotit)

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
git clone https://github.com/soruly/shotit-worker.git
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
