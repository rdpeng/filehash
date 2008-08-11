\name{stackqueue}
\alias{stackqueue}
\alias{push}
\alias{pop}
\alias{mpush}
\alias{top}
\alias{isEmpty}

%- Also NEED an '\alias' for EACH other topic documented here.
\title{Operations on Stacks/Queues}
\description{
  Functions for interacting with stack and queue data structures
  implemented using \code{filehash} databases.
}
\usage{
push(db, val, ...)
mpush(db, vals, ...)
pop(db, ...)
top(db, ...)
isEmpty(db, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{db}{an object of class \code{"stack"} or \code{"queue"}}
  \item{val}{an R object}
  \item{vals}{a list of R objects}
  \item{\dots}{arguments passed to other methods}
}
\details{
  Note that for \code{mpush}, if \code{vals} is not a list it will be
  coerced to a list via \code{as.list}.  Currently, \code{mpush} is only
  implemented for \code{"stack"}s.
}
\value{
  \code{push} and \code{mpush} return nothing useful; \code{pop} returns
  a value from the stack/queue and deletes that value from the
  stack/queue; \code{top} returns the "top" value from the stack/queue;
  \code{isEmpty} returns \code{TRUE}/\code{FALSE} depending on whether
  the stack/queue is empty or not.  Both \code{pop} and \code{top}
  signal an error if the stack/queue is empty.
}
\author{Roger D. Peng \email{rpeng@jhsph.edu}}
\keyword{database}% __ONLY ONE__ keyword per line
