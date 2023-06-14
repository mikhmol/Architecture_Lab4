package main

import (
	"testing"

	check "gopkg.in/check.v1"
)

// func TestBalancer(t *testing.T) {
// 	// TODO: Реалізуйте юніт-тест для балансувальникка.
// }

func Test(t *testing.T) { check.TestingT(t) }

type MySuite struct{}

var _ = check.Suite(&MySuite{})

func (s *MySuite) TestGetMinByteServer(c *check.C) {
	// Given
	serverBytes["server1:8080"] = 500
	serverBytes["server2:8080"] = 200
	serverBytes["server3:8080"] = 300

	// When
	minServer := getMinByteServer()

	// Then
	c.Assert(minServer, check.Equals, "server2:8080")
}
