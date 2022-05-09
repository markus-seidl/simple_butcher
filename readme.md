# What is it?

A simple backup for LTO tape drives which allows simpler restoring than the usual `tar -f /dev/nst0` backup method with CPU encryption and CPU compression.

The simple tar backup method has the problem, that CPU compression/encryption treats the whole backup as a single file. Meaning that if a single backup spans multiple tapes, all tapes are needed for restoration.
`simple_butcher` only needs a single tape, or a single segment for restoration (given that the file of interest is stored there of course).

Additionally no "custom code" is used, `simple_butcher` only orchestrates other commands, like:

* tar
  * Used to convert the files from disk into a binary stream. Also used to cut the stream into chunks (by default 5 or 10GB)
* 7z
  * Used for universal encryption in AES
* zstd
  * Used for fast and good compression 
* mbfuffer
* age
  * https://github.com/FiloSottile/age
* sha256sum
* md5sum
  * A lot faster than sha256

# FAQ

* Why is zstd + 7z used?

7z has a very and non-fast compression in default mode, but a very secure encryption scheme (AES256). Zstd has a fast compression

* Why are only segments written to tape, and no stream?

This has multiple reasons: The first, and simple one, is, that only command line tools should be used and simple_butcher mostly acts as advanced shell script. 
In the end there is no data handling by code, only shell commands
The second reason is, that the tape is broken up in chunks that are individually compressed and encrypted. This means, that in case of a partial restore, only 
the necessary chunks need to be decrypted and decompressed. Also, in case of tape failure or loss, not all tapes are needed to restore the files that are on the "good/remaining" tapes.

Additionally only fully ready chunks are written to the tape, preventing unnecessary spin up/down of the drive. The `mbuffer` command ensures 
a fast data delivery to the drive, if the hardware can handle it.
