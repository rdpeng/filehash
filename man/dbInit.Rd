\name{dbInit}
\alias{dbInit}
\alias{dbInitialize}
\alias{dbCreate}
\alias{dbCreate,ANY-method}
\alias{dbInit,ANY-method}
\alias{dbReconnect}
\alias{dbReconnect,filehashDB1-method}

%\alias{dbInitialize}

\title{Simple file-based hash table}
\description{
  Interface for creating and initializing a simple file-based hash table
}
\usage{
dbCreate(db, ...)
dbInit(db, ...)
dbReconnect(db, ...)

\S4method{dbCreate}{ANY}(db, type = NULL, ...)
\S4method{dbInit}{ANY}(db, type = NULL, ...)
\S4method{dbReconnect}{filehashDB1}(db, ...)
}

\arguments{
  \item{db}{name of database or a database object}
  \item{type}{type of database format.  If missing, the default type
    will be used}
  \item{...}{other arguments passed to methods}
}

\details{
  \code{dbCreate} creates the necessary files or directory for the
  database.  If those files already exist nothing is done.

  \code{dbInit} takes a database name and returns an object
  inheriting from class \code{"filehash"}.

  The \code{type} argument specifies the format in which the database
  should be stored on the disk.  If not specified, the default
  type will be used (as specified by \code{filehashOption}).  
}

\note{
  The function \code{dbInitialize} has been deprecated.  Use
  \code{dbInit} instead.
}

\value{
  \code{dbCreate} returns \code{TRUE} upon success and \code{FALSE} in
  the event of an error.  \code{dbInit} returns an object
  inheriting from class \code{"filehash"}
}

\author{Roger D. Peng}

\seealso{
  See \code{\link{filehash-class}} more information and examples and
  \code{\link{filehashOption}} for setting the default database type.
}

\keyword{database}% at least one, from doc/KEYWORDS

