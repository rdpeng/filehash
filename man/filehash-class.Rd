\name{filehash-class}
\docType{class}
\alias{filehash-class}
\alias{filehashDB-class}
\alias{filehashRDS-class}
\alias{filehashDB1-class}
\alias{dbFetch}
\alias{dbMultiFetch}
\alias{dbInsert}
\alias{dbExists}
\alias{dbList}
\alias{dbDelete}
\alias{dbReorganize}
\alias{dbUnlink}
\alias{dbDelete,filehashDB,character-method}
\alias{dbExists,filehashDB,character-method}
\alias{dbFetch,filehashDB,character-method}
\alias{dbInsert,filehashDB,character-method}
\alias{dbList,filehashDB-method}
\alias{dbUnlink,filehashDB-method}
\alias{dbReorganize,filehashDB-method}
\alias{dbMultiFetch,filehashDB1-method}
\alias{dbDelete,filehashDB1,character-method}
\alias{dbExists,filehashDB1,character-method}
\alias{dbFetch,filehashDB1,character-method}
\alias{dbMultiFetch,filehashDB1,character-method}
\alias{dbInsert,filehashDB1,character-method}
\alias{dbList,filehashDB1-method}
\alias{dbUnlink,filehashDB1-method}
\alias{dbReorganize,filehashDB1-method}
\alias{dbDelete,filehashRDS,character-method}
\alias{dbExists,filehashRDS,character-method}
\alias{dbFetch,filehashRDS,character-method}
\alias{dbMultiFetch,filehashRDS,character-method}
\alias{dbInsert,filehashRDS,character-method}
\alias{dbList,filehashRDS-method}
\alias{dbUnlink,filehashRDS-method}
\alias{show,filehash-method}
\alias{with,filehash-method}
\alias{coerce,filehashDB,filehashRDS-method}
\alias{coerce,filehashRDS,filehashDB-method}
\alias{coerce,filehashDB1,filehashRDS-method}
\alias{coerce,filehashDB1,list-method}
\alias{coerce,filehashDB,filehashDB1-method}
\alias{coerce,filehash,list-method}
\alias{lapply,filehash-method}
\alias{names,filehash-method}
\alias{length,filehash-method}

\alias{[,filehash,character,missing,missing-method}
\alias{[[,filehash,character,missing-method}
\alias{[[,filehash,numeric,missing-method}
\alias{[[<-,filehash,character,missing-method}
\alias{[[<-,filehash,numeric,missing-method}
\alias{$<-,filehash-method}
\alias{$,filehash-method}

\title{Class "filehash"}

\description{
  These functions form the interface for a simple file-based key-value
  database (i.e. hash table).
}

\section{Objects from the Class}{
  Objects can be created by calls of the form \code{new("filehash", ...)}.
}

\section{Slots}{
  \describe{
    \item{\code{name}:}{Object of class \code{"character"}, name of the
      database.}
  }
}

\section{Additional slots for "filehashDB1"}{
  \describe{
    \item{\code{datafile}:}{full path to the database file.}
    \item{\code{meta}:}{list containing an environment for database
      metadata.}
  }
}

\section{Additional slots for "filehashRDS"}{
  \describe{
    \item{dir:}{Directory where files are stored.}
  }
}

\section{Methods}{
  \describe{
    \item{dbDelete}{The \code{dbDelete} function is for deleting
      elements, but for the \code{"DB1"} format all it does is remove the
      key from the lookup table. 
      The actual data are still in the database (but inaccessible).  If
      you reinsert data for the same key, the new data are simply
      appended on to the end of the file.  Therefore, it's possible to
      have multiple copies of data lying around after a while,
      potentially making the database file big.  The \code{"RDS"} format
      does not have this problem.}
    \item{dbExists}{check to see if a key exists.}
    \item{dbFetch}{retrieve the value associated with a given key.}
    \item{dbMultiFetch}{retrieve values associated with multiple keys (a
      list of those values is returned).}
    \item{dbInsert}{insert a key-value pair into the database.  If
      that key already exists, its associated value is overwritten. For
      \code{"RDS"} type databases, there is a \code{safe} option
      (defaults to \code{TRUE}) which allows the user to insert objects
      somewhat more safely (objects should not be lost in the event of
      an interrupt).}
    \item{dbList}{list all keys in the database.}
    \item{dbReorganize}{The \code{dbReorganize} function is there for
      the purpose of rewriting the database to remove all of the stale
      entries.  Basically, this function creates a new copy of the
      database and then overwrites the old copy.  This function has not
      been tested extensively and so should be considered
      \emph{experimental}.  \code{dbReorganize} is not needed when using
      the \code{"RDS"} format.}
    \item{dbUnlink}{delete an entire database from the disk}
    \item{show}{print method}
    \item{with}{allows \code{with} to be used with \code{"filehash"}
      objects much like it can be used with lists or data frames}
    \item{[[,[[<-}{elements of a database can be accessed using the \code{[[}
      operator much like a list or environment, but only character
      indices are allowed}
    \item{$,$<-}{elements of a database can be accessed using the \code{$}
      operator much like with a list or environment}
    \item{lapply}{works much like \code{lapply} with lists; a list is
      returned.}
    \item{names}{returns all of the keys in the database}
    \item{length}{returns the number of elements in the database}
  }
}

\author{Roger D. Peng \email{rpeng@jhsph.edu}}

\examples{
dbCreate("myDB")  ## Create database 'myDB'
db <- dbInit("myDB")
dbInsert(db, "a", 1:10)
dbInsert(db, "b", rnorm(1000))
dbExists(db, "b")  ## 'TRUE'

dbList(db)  ## c("a", "b")
dbDelete(db, "a")
dbList(db) ## "b"

with(db, mean(b))
}
\keyword{classes}
