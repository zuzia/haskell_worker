Haskell Disco Worker (work in progress)
=======================================
In order to write map-reduce jobs in haskell, you have to first compile your haskell worker.
First, install the aeson package for parsing json and http for http operations.
Then the example can be built with:

```
$ ghc --make Word_count.hs -o word_count -i../src
```

Then you can use the jobpack utility to send the compiled binary to a disco master. For example:

```
$ go get github.com/discoproject/goworker/jobpack
$ $GOPATH/bin/jobpack -W word_count -I http://discoproject.org/media/text/chekhov.txt
```

Build Status: [Travis-CI](http://travis-ci.org/zuzia/haskell_worker) ::
![Travis-CI](https://secure.travis-ci.org/zuzia/haskell_worker.png)
