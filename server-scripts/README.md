This toolset is used on my VPS - an automated pipeline
that downloads sequential archive snapshots from murolem GitHub
and calculates the binary difference between them.
The resulting incremental diff files are then compressed and uploaded to a separate GitHub repository
for distribution.