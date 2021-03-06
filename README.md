## Work in Progress

Successor to [backitup](https://github.com/shanegibbs/backitup) (Jul 19, 2015), [babirusa](https://github.com/shanegibbs/babirusa) (Sep 11, 2014) and a few others that did not make it online.

## TODO

### Primary
* S3 upload backup_set records
* Full integration testing

### Secondary
* Thread up full scan
* Use transactions for updates
* Add tests for cli/config.yml interface
* Handle sym/hard links
* Collapse backup sets
* Look into refactoring `Node` such that `validate()` is redundant
* Overhaul errors
* Handle database locked errors
* Encryption
* Remove un-indexed hashes from store.

## Done
* ~~Stage Nodes to disk before inserting in DB. Only insert once backup run is complete.~~
* ~~S3 upload hashes~~
* ~~Refactor `Storage.send` to `Storage.send(Read, &[u8])`~~
* ~~Max file size option~~
* ~~Config file~~
* ~~Restore and list to a specific timestamp~~
* ~~List~~
* ~~Restore~~
* ~~Storage verification~~
* ~~Backup Sets~~
* ~~Predictable backup times~~
* ~~Backup to local directory~~
* ~~Scan and monitor all basic file and dir changes~~
* ~~Use SQLite for index~~

# Using timestamps

```
haumaru ls -w target/work -k myproject@$(date -u -v-8d +'%s')
```

## Engine

Single thread IO read.
Multi thread hashing
Callback on complete/error

# Linux

```
cat /proc/sys/fs/inotify/max_user_watches
sudo sysctl fs.inotify.max_user_watches=524288
```