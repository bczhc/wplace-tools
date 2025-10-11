wplace-tools
--

Tools for [wplace-archives](https://github.com/murolem/wplace-archives).

TODO

# Outdated below...

## Building

```shell
cargo build -r
```

## Creating incremental deltas

As an example: there are two world archives retrieved
from [wplace-archives](https://github.com/murolem/wplace-archives):

```
9.2G	2025-09-21T06-32-28.284Z+3h2m    (1)
9.1G	2025-09-21T09-35-13.789Z+2h49m   (2)
```

For creating an incremental backup of (2), with (1) as its parent, do:

```shell
parent='2025-09-21T06-32-28.284Z+3h2m'
archive='2025-09-21T09-35-13.789Z+2h49m'
target/release/archive-tool diff "$parent" "$archive" ./diff.bin
```

Standalone file `diff.bin` saves all the changes from archive (1) to (2). Its size
is small a lot.

```console
❯ du -sh diff.bin
34M	diff.bin
```

## Applying incremental data

```shell
parent='2025-09-21T06-32-28.284Z+3h2m'
archive='restored'
target/release/archive-tool apply "$parent" ./diff.bin "$archive"
```

Archive `2025-09-21T09-35-13.789Z+2h49m` will be restored.

You can use `archive-tool compare` to verify the restoration.

```shell
target/release/archive-tool compare \
  '2025-09-21T09-35-13.789Z+2h49m' \
  'restored'
```

No error will encounter. The two archives are identical.

## Other scripts

**Tile number to lat/lng**

```shell
bun run tile-to-coord.js <tile-x> <tile-y>
```
