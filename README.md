Haskell Disco Worker (work in progress)
=======================================
In order to write map-reduce jobs in haskell, you have to first compile your haskell worker. For example:
```
$ ghc --make Word_count.hs -o word_count
```

Then you can use the jobpack utility to send the compiled binary to a disco master. For example:

```
$ go get github.com/discoproject/goworker/jobpack
$ $GOPATH/bin/jobpack -W word_count -I http://discoproject.org/media/text/chekhov.txt
```
Build-depends:      base, text, bytestring, HTTP, vector, unix, aeson, directory

