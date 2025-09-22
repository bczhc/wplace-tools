wplace-tools
--

Tools for [wplace-archives](https://github.com/murolem/wplace-archives).

## Building

```shell
cargo build -r
```

## Creating incremental deltas

As an example: there are two world archives retrieved
from [wplace-archives](https://github.com/murolem/wplace-archives):

```
5.0G	2025-09-04T10-21-29.961Z+2h59m (1)
5.2G	2025-09-04T13-20-46.618Z+3h0m (2)
```

For creating an incremental backup of (2), with (1) as its parent, do:

```shell
parent='2025-09-04T10-21-29.961Z+2h59m'
archive='2025-09-04T13-20-46.618Z+3h0m'
target/release/archive-tool diff "$parent" "$archive" ./diff
```

An extra compression needs to be done. This reduces disk usage further.

```console
❯ tar -c diff | pigz > diff.tgz
❯ du -sh diff.tgz
72M	diff.tgz
```

Now we only have a 72M incremental data pack.

## Applying incremental data

First extract the diff pack.

```shell
tar -xzf diff.tgz
```

Then apply the changes.

```shell
parent='2025-09-04T10-21-29.961Z+2h59m'
archive='2025-09-04T13-20-46.618Z+3h0m-restored'
target/release/archive-tool apply "$parent" ./diff "$archive"
```

Archive `2025-09-04T13-20-46.618Z+3h0m` will be restored.

You can use `archive-tool compare` to verify the restoration process.

```shell
target/release/archive-tool compare \
  '2025-09-04T13-20-46.618Z+3h0m' \
  '2025-09-04T13-20-46.618Z+3h0m-restored'
```

No error will encounter. The two archives are identical.

## Other scripts

**Tile number to lat/lng**

```shell
bun run tile-to-coord.js <tile-x> <tile-y>
```
