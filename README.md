## Work in Progress

Sucessor to [backitup](https://github.com/shanegibbs/backitup) (Jul 19, 2015), [babirusa](https://github.com/shanegibbs/babirusa) (Sep 11, 2014) and a few others that did not make it online.

## TODO

### Primary
* Restore
* List
* Config file
* S3 upload (hashes and backup_set records)
* Refactor `Storage.send` to `Storage.send(Read, &[u8])`
* Use transactions for updates

### Secondary
* Handle sym/hard links
* Colapse backup sets
* Look into refactoring `Node` such that `validate()` is redundant
* Overhaul errors
* Handle database locked errors
* Encryption

## Done

* ~~Storage verification~~
* ~~Backup Sets~~
* ~~Predictable backup times~~
* ~~Backup to local directory~~
* ~~Scan and monitor all basic file and dir changes~~
* ~~Use SQLite for index~~
