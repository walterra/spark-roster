//go:build !linux

package main

import "errors"

func realEnv() (env, error) {
	return env{}, errors.New("profile commands need Linux; use list, plan, memory and storage elsewhere")
}
