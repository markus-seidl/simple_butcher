# What is it?

A simple backup for LTO tape drives which allows simpler restoring than the usual `tar -f /dev/nst0` backup method with
CPU encryption and CPU compression.

The simple tar backup method has the problem, tar (+compression/encryption) treats the whole backup as a single file.
Meaning that if a single backup spans multiple tapes, all tapes are needed for restoration (!).
`simple_butcher` splits the stream up in chunks which are encrypted and compressed separately and can therefore be
restored separately as well. Only the tape/chunk is needed that contains the files that will be restored.
The data is written to a scratch drive beforehand, which should be able to keep up with the tape drive.

Additionally, no "custom code" is used, `simple_butcher` only orchestrates other commands, like:

* tar
    * Used to convert the files from disk into a binary stream. Also used to cut the stream into chunks
* zstd
    * Used for fast and good compression on multiple cores
* mbuffer
    * Used to write to the tape via a memory buffer so the drive doesn't scrub the tape
* age
    * https://github.com/FiloSottile/age
* md5sum
    * A lot faster than sha256 and should be enough to detect errors and identify chunks (zstd, age and tar have their
      own checksums)

# FAQ

* Installation

1) Download repository
2) Install requirements: `tar`, `zstd`, `age` (>= 1.0.0), `md5sum`, `mbuffer`, `python3`
   (> 3.8), `mt-st`, `tapeinfo`, `sg_logs`
    * Often you can just write the command in the shell and the system will tell you what package to install
3) Make sure your tape drive works with `mt-st` (e.g. `mt-st -f /dev/nst0 status`)
4) Install python requirements: `pip3 install -r requirements.txt` (pip or pip3, depending on your system)
    * My recommendation would be to use a venv
      environment: `python3 -m venv venv && source venv/bin/activate && pip3 install -r requirements.txt --upgrade`
    * Venv can be activated with `source venv/bin/activate` and deactivated with `deactivate`
5) Run `simple_butcher` with `./run.sh --help`

* Why are only segments written to tape, and no stream handled by python?

This has multiple reasons: The first, and simple one, is, that only command line tools should be used and
`simple_butcher` mostly acts as advanced shell script. In the end there is no data handling by code, only shell commands
The second reason is, that the tape is broken up in chunks that are individually compressed and encrypted.
This means, that in case of a partial restore, only the necessary chunks need to be decrypted and decompressed.
Also, in case of tape failure or loss, not all tapes are needed to restore the files that are on the
"good/remaining" tapes.

Only fully ready chunks are written to the tape, preventing unnecessary spin up/down of the drive.
The `mbuffer` command ensures a fast data delivery to the drive, if the hardware can handle it.
In my tests at least a NVMe SSD is needed to keep up with the tape drive. LTO-6 drives can write at 160 MB/s, whereas

* What if I loose the sources to `simple_butcher`?
  That is not a problem, only the encryption key and the tape are needed to restore the files. All used tools are
  open source and can be downloaded easily in the future. The following steps are needed to restore the files:

1) Install the requirements: tar, zstd, age, md5sum, mbuffer (which are needed for `simple_butcher` as well)
2) Dump the tape to a file: `mbuffer -i /dev/nst0 -o restore_chunk.0001.zstd.age --tapeaware -s 512K`
   (this is needed for every chunk on that tape) (-s is important, otherwise the data will be garbage!)
3) Decrypt with age: `age -d -i keyfile.txt -o restore_chunk.0001.zstd.age restore_chunk.0001.zstd`
4) Extract with zstd: `zstd -d -o restore_chunk.0001.tar restore_chunk.0001.zstd`
5) Extract with tar: `tar xvf restore_chunk.0001.tar`
6) Repeat steps 2-5 for every chunk on the tape

* What is currently supported?

* Full backups of a directory
* Full restores (all files on the backup)
