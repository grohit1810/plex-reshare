# PLEX-reshare

Combo of [openresty](https://openresty.org/) + [starlette](https://www.starlette.io/) + [rq](https://python-rq.org) + [redis](https://redis.io/) + [sqlite](https://www.sqlite.org/) to expose your Plex shares in a basic web-browsable `:8080`  format a'la apache directory listing.

The reason behind this project it to make available your PLEX shares to other friends unrelated to the person who owns the original library.

For example, `plex-server-A` shares various libraries (Movies & TV Shows supported) to (your) `plex-server-owned-by-you-B`
using `plex-reshare` create a new local library with the files from `plex-server-A` that you can later on can be shared directly to other friends `C`.

Basically `plex-reshare` will act as a plex-library-proxy and all the traffic will pass through it (downstream+upstream). It's ignoring self-libraries.


# Use scenario

You managed to get access to one or more shared libraries from other servers, with plex-reshare as a proxy you can host your own instance of plex and share it back to other close friends.

PS: it's not mandatory to use plex to share further the access, by same principle you can also use Jellyfin/Emby/any other media manager-indexer or even simply direct http access and open urls directly in VLC/IINA for example.

USE WITH CARE, **DO NOT HEAVILY REQUEST DATA FROM TARGET SERVERS**. BE NICE!

It'll create a http directory listing under the format

```
/
|-- movies
|   |-- c0e5a2..........................
|   |   |-- movie.libraryA.movie1
|   |   |-- movie.libraryA.movie2
|   |   `-- movie.libraryB.movie1
|   |-- cb7d61..........................
|   |-- e82c68..........................
|   `-- f2423f..........................
`-- shows
    |-- c0e5a2..........................
    |   |-- tvshow.libraryA.show1
    |   |-- tvshow.libraryA.show2
    |   `-- tvshow.libraryB.show1
    |-- cb7d61..........................
    |-- e82c68..........................
    `-- f2423f..........................
```

All the movie/shows libraries exposed by a specific plex server will be listed all in one place under a single served id uniquely identifiable.

As of now it's not made to recreate the structure defined by a specific plex(admin) but more like grouping all the data available and use external option like PMM (Plex Meta Manager) to create a more structured format out of (subject to change if needed/requested, please fill an issue!).


# How it works

The catalog is built and served in three layers:

- **Crawl (rq worker).** Discovers the servers shared with your token, then walks each
  server's movie and TV libraries. TV shows are fetched with Plex's `allLeaves`
  endpoint — every episode of a show in a single request — instead of walking season
  by season. Movies and shows are stored in a local **SQLite** database, which is the
  durable source of truth for the catalog.
- **Serve (openresty + redis).** The browsable listing at `:8080` is served from Redis.
  After each crawl the Redis listing is rebuilt from SQLite in one atomic swap, so a
  reader never sees a half-built catalog. Playback requests are reverse-proxied
  straight to the origin Plex server; nothing is stored or transcoded here.
- **Persist (sqlite).** Because the catalog lives in SQLite, a restart repopulates Redis
  from the database instead of re-crawling the source servers.

**Refresh model.** Each server is re-crawled on a randomized interval
(`REFRESH_HOURS_MIN`–`REFRESH_HOURS_MAX`). Refreshes are incremental: a show is only
re-fetched when its episode count or `updatedAt` changes, so an unchanged library costs
almost no requests. A full from-scratch rebuild runs on a longer randomized interval
(`FULL_CRAWL_DAYS_MIN`–`FULL_CRAWL_DAYS_MAX`) to reconcile anything the incremental
checks might miss. Deletions are detected by diffing a complete crawl against the
database; a file that returns `404` on playback is also dropped from the listing on the
spot (`LAZY_DELETE_ON_404`).

> Please keep the defaults conservative and **do not heavily request data from the
> origin servers**. Be nice!

# Installation via Docker

Docker images available https://hub.docker.com/r/peterbuga/plex-reshare

### Sample command
```
docker run -d --name=plex-reshare \
-e PLEX_TOKEN='xxxxxxxxxxxxxxx'
-p 8080:8080 \
peterbuga/plex-reshare:latest
```

### Docker compose

Copy `.env.sample` to `.env` and change the variable accordingly.

`docker compose up -d`

Browse to http://your-host-ip:8080 to access the list of plex reshares.

### Environment variables

| Variable       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                       | Default |
| ---------------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --------- |
|`PLEX_TOKEN`| (mandatory) find out [how to get a plex token](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/).                                                                                                                                                                                                                                                                                                         | (unset) |
|`REDIS_INTERNAL`| (optional) `true` or `false` use internal redis instance to store the files structure, using an internal refreshing system                                                                                                                                                                                                                                                                                                                        | `true` |
|`REDIS_HOST`| (optional) option to use an external redis instance if already available, set `REDIS_INTERNAL: false`                                                                                                                                                                                                                                                                                                                                             | `127.0.0.1` |
|`REDIS_PORT`| (optional) to used when `REDIS_INTERNAL: false`                                                                                                                                                                                                                                                                                                                                                                                                   | `11` |
|`REDIS_DB_RQ`| (optional) if python-rq should run on a separate redis db                                                                                                                                                                                                                                                                                                                                                                                         | `11` |
|`IGNORE_PLAYLIST`| (optional) name of a playlist whose items should be excluded from the listing                                                                                                                                                                                                                                                                                                                                                                                  | (unset) |
|`IGNORE_RESOLUTIONS`| (optional) list of resolutions comma separated that you'd like to ignore, ex: `sd` | (unset) |
|`IGNORE_EXTENSIONS`| (optional) list of file extension comma separated that you'd like to ignore, ex: `avi,mpeg` | (unset) |
|`MOVIE_MIN_SIZE`| (optional) minimal file size of a movie in Mb, everything below will be ignored | 512 |
|`EPISODE_MIN_SIZE`| (optional)  minimal file size of an episode in Mb, everything below will be ignored | 64 |
|`IGNORE_MOVIE_TEMPLATES`| (optional) list of python regexes to ignore being added to the list, pipe (`\|`) separated, ex: `.*sample.*` will ignore all the sample file sometimes associated with movie files | (unset) |
|`IGNORE_EPISODE_TEMPLATES`| (optional) list of python regexes to ignore being added to the list, pipe (`\|`) separated | (unset) |
|`PLEX_CONTAINER_SIZE`| (optional) items fetched per page when crawling; larger means fewer requests | `400` |
|`REFRESH_HOURS_MIN` / `REFRESH_HOURS_MAX`| (optional) a server is re-crawled after a random gap in this hour range | `5` / `12` |
|`FULL_CRAWL_DAYS_MIN` / `FULL_CRAWL_DAYS_MAX`| (optional) a full from-scratch rebuild runs after a random gap in this day range | `6` / `9` |
|`LAZY_DELETE_ON_404`| (optional) drop a listing entry immediately when playback returns `404` | `true` |
|`LOG_LEVEL`| (optional) `DEBUG` for per-request tracing, `INFO` for summaries | `INFO` |
|`LOG_FILE`| (optional) also write logs to this file (e.g. `/pr/plex-reshare.log`); disabled if unset | (unset) |


# Local image build
The build image is a merge of multiple external dockerfiles (in order to kickstart the developent) that's why there's no local Dockerfile defined

`make build`

# Rclone mount
More details here https://rclone.org/http/ but I recommand using flags `--transfers 4 --low-level-retries 7 --retries 7 --tpslimit 0.7 ` to limit the access to API and files, otherwise plex scan will hammer the requests on target libraries.

# Development
### Linting:
Requires `pip install ruff==0.3.0`

`make format-code`

# Credits
- https://github.com/openresty/docker-openresty
- https://github.com/tiangolo/uvicorn-gunicorn-docker

# License
MIT license
