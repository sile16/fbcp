# fbcp
FlashBlade Fast File transfer tool


Calculate correct hash using:
for x in `ls -1` ; do echo $x ; xxhsum -q -H3 $x | awk '{print $4}' | xxd -r -p | xxhsum -H3 ; done
it does a double hash on each file.  This should be accurate for all single threaded versions of the tool.
