//go:build mage
// +build mage

package main

import (
	//mage:import docs
	_ "github.com/ttab/mage/docs"
	//mage:import s3
	_ "github.com/ttab/mage/s3"
	//mage:import sql
	_ "github.com/ttab/mage/sql"
)
