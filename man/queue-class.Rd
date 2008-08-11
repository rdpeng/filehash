\name{queue-class}
\docType{class}
\alias{queue-class}
\alias{isEmpty,queue-method}
\alias{pop,queue-method}
\alias{push,queue-method}
\alias{show,queue-method}
\alias{top,queue-method}

\title{Class "queue"}
\description{A queue implementation using a \code{filehash} database}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("queue", ...)} or
by calling \code{createQ}.  Existing queues can be initialized with
\code{initQ}.
}
\section{Slots}{
	 \describe{
    \item{\code{queue}:}{Object of class \code{"filehashDB1"}}
    \item{\code{name}:}{Object of class \code{"character"}: the name of
      the queue (default is the file name in which the queue data are
      stored)}
  }
}
\section{Methods}{
  \describe{
    \item{isEmpty}{\code{signature(db = "queue")}: returns
      \code{TRUE}/\code{FALSE} depending on whether there are elements
      in the queue.}
    \item{pop}{\code{signature(db = "queue")}: returns the value of the
      "top" (i.e. head) of the queue and subsequently removes that
      element from the queue; an error is signaled if the queue is empty}
    \item{push}{\code{signature(db = "queue")}: adds an element to the
      tail ("bottom") of the queue}
    \item{show}{\code{signature(object = "queue")}: prints the name of
      the queue}
    \item{top}{\code{signature(db = "queue")}: returns the value of the
      "top" (i.e. head) of the queue; an error is signaled if the queue
      is empty}
  }
}
\author{Roger D. Peng \email{rpeng@jhsph.edu}}

\examples{
showClass("queue")
}
\keyword{classes}
