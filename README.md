wplace-tools
===

A collection of CLI utilities for working with snapshots from [wplace-archives](https://github.com/murolem/wplace-archives): creating diffs, applying them, and retrieving chunk images.

## Building project

```shell
cargo build -r
```

Binaries are present in `target/release`.

## Creating delta between two snapshots

As an example: there are two world archives retrieved
from [wplace-archives](https://github.com/murolem/wplace-archives):

```
9.2G	2025-09-21T06-32-28.284Z+3h2m    (1)
9.1G	2025-09-21T09-35-13.789Z+2h49m   (2)
```

To create delta from snapshot (1) -> (2), do:

```shell
parent='2025-09-21T06-32-28.284Z+3h2m'
archive='2025-09-21T09-35-13.789Z+2h49m'
archive-tool diff "$parent" "$archive" ./diff.bin
```

Tarball inputs are also supported:

```shell
parent='2025-09-21T06-32-28.284Z+3h2m.tar'
archive='2025-09-21T09-35-13.789Z+2h49m.tar'
archive-tool diff "$parent" "$archive" ./diff.bin
```

Standalone file `diff.bin` saves all the changes from archive (1) to (2).

## Applying diff data

Reconstruct the snapshot from its parent and a `.diff` file:

```shell
parent='2025-09-21T06-32-28.284Z+3h2m'
archive='2025-09-21T09-35-13.789Z+2h49m'

# delete the archive
rm -rf "$archive"

# then restore it
archive-tool apply "$parent" diff.bin "$archive"
```

Archive `2025-09-21T09-35-13.789Z+2h49m` will be restored.

## Wplace incremental backup

An [**incremental backup**](https://en.wikipedia.org/wiki/Incremental_backup) is one in which successive copies of the data contain only the portion that has changed since the preceding backup copy was made. That is, only an initial snapshot and all its later consecutive diff files need to be saved.

By the convention, name of all Wplace snapshots and diff files should be in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) UTC datetime, e.g. `2025-09-21T06-32-28.284Z/` or `2025-09-21T06-32-28.284Z.tar` or `2025-09-21T06-32-28.284Z.diff`.

Assume we have Wplace snapshots tarballs formed as below:

```shell
snap1=2025-08-09T20-01-14.231Z.tar
snap2=2025-08-09T22-23-45.217Z.tar
snap3=2025-08-10T00-50-04.021Z.tar
snap4=2025-08-10T03-23-13.303Z.tar
snap5=2025-08-10T05-54-10.072Z.tar
```

To create an incremental chain:

```shell
mkdir diff-folder
archive-tool diff $snap1 $snap2 diff-folder/2025-08-09T22-23-45.217Z.diff
archive-tool diff $snap2 $snap3 diff-folder/2025-08-10T00-50-04.021Z.diff
archive-tool diff $snap3 $snap4 diff-folder/2025-08-10T03-23-13.303Z.diff
archive-tool diff $snap4 $snap5 diff-folder/2025-08-10T05-54-10.072Z.diff

# Only base snapshot `2025-08-09T20-01-14.231Z.tar` and folder `diff-folder` are what we need.
# Full backups after the initial one are not needed anymore; optionally delete them.
rm $snap2 $snap3 $snap4 $snap5
```

Through incremental backup, one can store all Wplace snapshots locally, with a small disk usage.

### Retrieving chunk images

#### CLI usage

<pre>Chunk image retrieval tool
<u style="text-decoration-style:solid"><b>Usage:</b></u> <b>retrieve</b> [OPTIONS] <b>--chunk</b> &lt;CHUNK&gt; <b>--diff-dir</b> &lt;DIFF_DIR&gt; <b>--base-snapshot</b> &lt;BASE_SNAPSHOT&gt; <b>--out</b> &lt;OUT&gt;
<u style="text-decoration-style:solid"><b>Options:</b></u>
  <b>-c</b>, <b>--chunk</b> &lt;CHUNK&gt;                  Chunk(s) to retrieve. Format: x1-y1,x2-y2,x3-y3,... or x1-y1..x2-y2
  <b>-d</b>, <b>--diff-dir</b> &lt;DIFF_DIR&gt;            Directory containing all the consecutive .diff files
  <b>-b</b>, <b>--base-snapshot</b> &lt;BASE_SNAPSHOT&gt;  Path to the initial snapshot (tarball format)
  <b>-o</b>, <b>--out</b> &lt;OUT&gt;                      Output path
  <b>-t</b>, <b>--at</b> &lt;AT&gt;                        Snapshot name of the restoration point. If not present, use the newest one in `diff_dir`
  <b>-a</b>, <b>--all</b>                            If enabled, instead of retrieving only the target one, also retrieve all chunks prior to it
      <b>--disable-csum</b>                   Disable checksum validation. Only for debugging purposes
  <b>-s</b>, <b>--stitch</b>                         Stitch chunks together to a big image
  <b>-h</b>, <b>--help</b>                           Print help (see more with &apos;--help&apos;)
  <b>-V</b>, <b>--version</b>                        Print version</pre>

#### Examples

- Extract a single chunk (602, 0) at a specific snapshot point:

  ```shell
  retrieve -c 602-0 -d diff-folder -o output \
    -b 2025-08-09T20-01-14.231Z.tar \
    -t 2025-08-10T03-23-13.303Z
  ```

- Extract region (601, 0) - (603, 1) and stitch them together.

  ```shell
  retrieve -c 601-0..603-1 -d diff-folder -o output -s \
    -b 2025-08-09T20-01-14.231Z.tar
  ```

- Extract all historical chunk images for region (601, 0) - (603, 1) and generate a timelapse video.

  ```shell
  retrieve -c 601-0..603-1 -d diff-folder -o output -as \
    -b 2025-08-09T20-01-14.231Z.tar
  
  ffmpeg -framerate 15 -pattern_type glob \
    -i 'output/stitched/*.png' \
    -c:v libx264 -b:v 0 \
    -pix_fmt yuv420p \
    output.mp4
  ```

  This produces this: (as of `2025-10-10T01-05-20.144Z`)

  https://github.com/user-attachments/assets/fdd2942f-7c5f-4fe9-a676-89f89d55ba97