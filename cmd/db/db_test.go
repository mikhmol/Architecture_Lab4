package main

import (
	"testing"

	check "gopkg.in/check.v1"
)

// func TestDb(t *testing.T) {
// 	// TODO: Реалізуйте юніт-тест для балансувальникка.
// }

func Test(t *testing.T) { check.TestingT(t) }

type MySuite struct{}

var _ = check.Suite(&MySuite{})
