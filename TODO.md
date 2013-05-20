# RDS2

## Get the object list updating properly

http://stackoverflow.com/questions/3412942/in-r-how-can-one-make-a-method-of-an-s4-object-that-directly-adjusts-the-values

## Options
### Option Storage
Should these live an object specific metadata or in filehashOption()?
### Compression
  * Nice option to be able to turn on/off (per list?)
  * Note that gzfile can cope with uncompressed files as well so this only
    needs to be set on write in theory.

### xdr
see ?serialize
to avoid byte swapping


