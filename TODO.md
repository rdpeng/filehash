# RDS2

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

## Optimisation

Load test data set:

    sample_data=url('http://flybrain.mrc-lmb.cam.ac.uk/vfb/r/dps100.rda')
    load(sample_data)
    # beef that up a bit for testing
    set.seed(42)
    dps1000=sample(dps100,1000,replace=T)
    names(dps1000)=make.unique(names(dps1000))

Some quick tests:

    library(filehash)
    system.time(test1k.rds2<-dumpList(dps1000,'test1k.rds2',type='RDS2'))
    system.time(z<-dbMultiFetch(test1k.rds2,names(dps1000)))
    system.time(test1k.rds<-dumpList(dps1000,'test1k.rds',type='RDS'))
    system.time(z<-dbMultiFetch(test1k.rds,names(dps1000)))

Initial impression is that saving is ~ 5x faster and dominated by something other than serialize.
Read appears to be ~ 2-3x faster.
