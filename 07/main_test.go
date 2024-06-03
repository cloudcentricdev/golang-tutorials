package main

import (
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/cloudcentricdev/golang-tutorials/06/db"
)

var keys = []string{}

func init() {
	log.SetOutput(io.Discard)
}

func TestSSTSearch(t *testing.T) {
	d, err := db.Open("demo")
	if err != nil {
		log.Fatal(err)
	}

	for _, k := range keys {
		t.Run(fmt.Sprintln(k), func(t *testing.T) {
			_, err = d.Get([]byte(k))
			if err != nil {
				t.Fail()
			}
		})
	}

}

func BenchmarkSSTSearch(b *testing.B) {
	d, err := db.Open("demo")
	if err != nil {
		log.Fatal(err)
	}

	for _, k := range keys {
		b.Run(fmt.Sprintln(k), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err = d.Get([]byte(k))
				if err != nil {
					b.Fatal(err.Error())
				}
			}
		})
	}
}
