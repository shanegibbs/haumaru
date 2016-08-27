## Work in Progress

Sucessor to [backitup](https://github.com/shanegibbs/backitup) (Jul 19, 2015), [babirusa](https://github.com/shanegibbs/babirusa) (Sep 11, 2014) and a few others that did not make it online.

## TODO

### Primary
* Max file size option
* Refactor `Storage.send` to `Storage.send(Read, &[u8])`
* S3 upload (hashes and backup_set records)
* Use transactions for updates

### Secondary
* Handle sym/hard links
* Colapse backup sets
* Look into refactoring `Node` such that `validate()` is redundant
* Overhaul errors
* Handle database locked errors
* Encryption
* Remove un-indexed hashes from store.

## Done
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