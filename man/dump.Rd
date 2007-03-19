\name{dumpObjects}
\alias{dumpObjects}
\alias{dumpImage}
\alias{dumpDF}
\alias{dumpList}

\title{Dump objects of database}
\description{
  Dump R objects to a filehash database
}
\usage{
dumpObjects(..., list = character(0), dbName, type = NULL, envir = parent.frame())
dumpImage(dbName = "Rworkspace", type = NULL)
dumpDF(data, dbName = NULL, type = NULL)
dumpList(data, dbName = NULL, type = NULL)
}

\arguments{
  \item{\dots}{R objects to dump}
  \item{list}{character vector of names of objects to dump}
  \item{dbName}{character, name of database to which objects should be
    dumped}
  \item{type}{type of database to create}
  \item{envir}{environment from which to obtain objects}
  \item{data}{a data frame or a list}
}
\details{
  Objects dumped to a database can later be loaded via \code{dbLoad} or
  can be accessed with \code{dbFetch}, \code{dbList}, etc.
  Alternatively, the \code{with} method can be used to evaluate code in
  the context of a database.  If a database with name \code{dbName}
  already exists, objects will be inserted into the existing database
  (and values for already-existing keys will be overwritten).

  \code{dumpDF} is different in that each variable in the data frame is
  stored as a separate object in the database.  So each variable can be
  read from the database separately rather than having to load the
  entire data frame into memory.  \code{dumpList} works in a simlar
  way.
}

\value{
  An object of class \code{"filehash"} is returned and a database is
  created.
}

\author{Roger D. Peng}

\examples{
data <- data.frame(y = rnorm(100), x = rnorm(100), z = rnorm(100))
db <- dumpDF(data, dbName = "dataframe.dump")
fit <- with(db, lm(y ~ x + z))
summary(fit)

db <- dumpList(list(a = 1, b = 2, c = 3), "list.dump")
db$a
}
\keyword{database}% at least one, from doc/KEYWORDS

