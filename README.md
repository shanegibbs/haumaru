Sucessor to [backitup](https://github.com/shanegibbs/backitup) (Jul 19, 2015), [babirusa](https://github.com/shanegibbs/babirusa) (Sep 11, 2014) and a few others that did not make it online.

## TODO

### Primary
* Restore
* List
* S3 upload (hashes and backup_set records)
* Handle sym/hard links
* Look into refactoring `Node` such that `validate()` is redundant
* Use transactions for updates
* Handle database locked errors
* Config file
* Overhaul errors

### Secondary
* Colapse backup sets

## Done

* ~~Storage verification~~
* ~~Backup Sets~~
* ~~Backup to local directory~~
* ~~Scan and monitor all basic file and dir changes~~
* ~~Use SQLite for index~~
